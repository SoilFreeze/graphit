import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import re

# You will need your custom functions from your internal modules
from app.data.processor import get_universal_portal_data
from app.utils.config import natural_sort_key


#########################
# Page 3 - Depth Charts #
#########################
def render_depth_charts(selected_project, unit_label, display_tz):
    """
    Vertical Temperature Profiles.
    Maps arrays dynamically based on native view Depth allocations.
    """
    st.header(f"📏 Depth Profile Analysis: {selected_project}")
    
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view depth profiles.")
        return

    # THE FIX: Stop using a local slider. Read the global one from the sidebar.
    lookback_weeks = st.session_state.get("global_lookback_weeks_slider", 5)
    st.sidebar.caption(f"📏 Depth Charts using Global {lookback_weeks}-week window.")
    
    with st.spinner("Fetching historical telemetry..."):
        p_df = get_universal_portal_data(selected_project)
        
    if p_df is None or p_df.empty:
        st.warning("No data found for this project.")
        return

    # --- AUTO-FILTER BY PHASE FROM PROJECT TITLE ---
    import re
    phase_match = re.search(r'(?i)Phase\s*(\d+)', selected_project)
    
    if phase_match:
        target_phase = phase_match.group(1)
        p_df = p_df[p_df['Phase'].astype(str) == target_phase]
        st.sidebar.caption(f"🎯 Auto-filtered to Phase {target_phase}")

    # --- MANUAL SYSTEM FILTER (SIDEBAR) ---
    avail_systems = sorted([str(s) for s in p_df['System'].dropna().unique() if str(s).strip()])
    
    if avail_systems:
        sel_systems = st.sidebar.multiselect("Filter by System", avail_systems, default=avail_systems, key="depth_sys")
        if sel_systems:
            p_df = p_df[p_df['System'].astype(str).isin(sel_systems)]

    # Convert native view Depth values straight into a graph-safe float coordinate
    p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
    p_df = p_df[p_df['temperature'] <= 120.0]
    
    # --- INTERCEPT: Actively strip out Banks and Ambient sensors ---
    # Create safe, uppercase string columns for robust matching
    clean_loc = p_df['Location'].fillna('').astype(str).str.upper()
    clean_bank = p_df['Bank'].fillna('').astype(str).str.upper()

    # Flag any row that is Supply (S), Return (R), or Ambient (Amb)
    is_bank_or_amb = (
        clean_loc.str.startswith('S') | clean_loc.str.startswith('R') | clean_loc.str.contains('BANK') |
        clean_bank.str.startswith('S') | clean_bank.str.startswith('R') | clean_bank.str.contains('BANK') |
        clean_loc.str.contains('AMB') | clean_bank.str.contains('AMB')
    )
    
    # Drop the flagged rows before generating the final depth dataframe
    p_df = p_df[~is_bank_or_amb]
    # -------------------------------------------------------------
    
    depth_df = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
    
    if depth_df.empty:
        st.info("No Temp Pipe sensors with valid numeric 'Depth' entries found in the data stream.")
        return

    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    freeze_pt = 0 if unit_mode == "Celsius" else 32
    
    now_utc = pd.Timestamp.now(tz='UTC')
    
    # THE FIX 1: Calculate strict historical cutoff window based on the slider
    cutoff_date = now_utc - pd.Timedelta(weeks=lookback_weeks)
    mondays = pd.date_range(start=cutoff_date, end=now_utc, freq='W-MON')
    
    locations = sorted(depth_df['Location'].unique(), key=natural_sort_key)
    
    for loc in locations:
        with st.expander(f"📍 Temp vs Depth - {loc}", expanded=True):
            loc_data = depth_df[depth_df['Location'] == loc].copy()
            
            if loc_data['timestamp'].dt.tz is None:
                loc_data['timestamp'] = loc_data['timestamp'].dt.tz_localize('UTC')
            loc_data['timestamp_local'] = loc_data['timestamp'].dt.tz_convert(display_tz)
            
            fig = go.Figure()

            # --- A. BASELINE Snapshots (Always renders absolute oldest point) ---
            baseline_ts = loc_data['timestamp_local'].min()
            b_window = loc_data[
                (loc_data['timestamp_local'] >= baseline_ts - pd.Timedelta(hours=12)) & 
                (loc_data['timestamp_local'] <= baseline_ts + pd.Timedelta(hours=12))
            ]
            
            baseline_date_str = ""
            snap_base = pd.DataFrame()
            if not b_window.empty:
                baseline_date_str = baseline_ts.strftime('%Y-%m-%d')
                snap_base = (
                    b_window.assign(diff=(b_window['timestamp_local'] - baseline_ts).abs())
                    .sort_values(['NodeNum', 'diff'])
                    .drop_duplicates('NodeNum')
                    .sort_values('Depth_Num')
                )

            # --- B. RECENT 6 AM Snapshots ---
            loc_data['date_str'] = loc_data['timestamp_local'].dt.strftime('%Y-%m-%d')
            loc_data['hour_int'] = loc_data['timestamp_local'].dt.hour
            
            recent_6am_date_str = ""
            recent_profile_rows = []
            
            if not loc_data.empty:
                sorted_all_dates = sorted(loc_data['date_str'].unique(), reverse=True)
                
                for candidate_date in sorted_all_dates:
                    if candidate_date == baseline_date_str:
                        continue
                    
                    day_pool = loc_data[loc_data['date_str'] == candidate_date]
                    if day_pool.empty:
                        continue
                        
                    recent_6am_date_str = candidate_date
                    
                    for node_id, node_group in day_pool.groupby('NodeNum'):
                        exact_6am = node_group[node_group['hour_int'] == 6]
                        if not exact_6am.empty:
                            recent_profile_rows.append(exact_6am.sort_values('timestamp_local').iloc[-1])
                        else:
                            node_group = node_group.assign(hour_dist=(node_group['hour_int'] - 6).abs())
                            best_fallback_row = node_group.sort_values(by=['hour_dist', 'timestamp_local']).iloc[0]
                            recent_profile_rows.append(best_fallback_row)
                    break

            snap_recent = pd.DataFrame(recent_profile_rows).sort_values('Depth_Num') if recent_profile_rows else pd.DataFrame()

            # --- C. HISTORICAL SNAPSHOTS ---
            # THE FIX: We must use the 'mondays' list we calculated based on the slider, 
            # not a fixed iteration of periods.
            for m_date in mondays:
                target_ts = m_date.replace(hour=6, minute=0, second=0)
                current_loop_date = target_ts.strftime('%Y-%m-%d')
                
                # Skip if this date matches our special snapshots
                if current_loop_date == baseline_date_str or current_loop_date == recent_6am_date_str:
                    continue
                    
                # Search a 24-hour window around the Monday 6 AM mark
                window = loc_data[
                    (loc_data['timestamp_local'] >= target_ts - pd.Timedelta(hours=12)) & 
                    (loc_data['timestamp_local'] <= target_ts + pd.Timedelta(hours=12))
                ]
                
                if not window.empty:
                    # Pick the data point closest to 6 AM in that window
                    snap_week = (
                        window.assign(diff=(window['timestamp_local'] - target_ts).abs())
                        .sort_values(['NodeNum', 'diff'])
                        .drop_duplicates('NodeNum')
                        .sort_values('Depth_Num')
                    )
                    
                    temps = snap_week['temperature']
                    if unit_mode == "Celsius": temps = (temps - 32) * 5/9
                    
                    fig.add_trace(go.Scatter(
                        x=temps, y=snap_week['Depth_Num'], 
                        mode='lines+markers', 
                        name=current_loop_date,
                        line=dict(shape='spline', smoothing=1.1, width=1.5),
                        marker=dict(size=4),
                        hovertemplate=f"Date: {current_loop_date}<br>Depth: %{{y}}ft<br>Temp: %{{x:.1f}}{unit_label}<extra></extra>"
                    ))

            # --- D. INJECT THE MOST RECENT LINE ---
            if not snap_recent.empty:
                recent_temps = snap_recent['temperature']
                if unit_mode == "Celsius": recent_temps = (recent_temps - 32) * 5/9
                
                fig.add_trace(go.Scatter(
                    x=recent_temps, y=snap_recent['Depth_Num'],
                    mode='lines+markers',
                    name=f'<b>Most Recent ({recent_6am_date_str} 6AM*)</b>',
                    line=dict(color='#ff7f0e', width=3.5, shape='spline', smoothing=1.1),
                    marker=dict(size=6, color='#ff7f0e'),
                    hovertemplate="Most Recent: %{text}<br>Depth: %{y}ft<br>Temp: %{x:.1f}" + unit_label + "<extra></extra>",
                    text=snap_recent['timestamp_local'].dt.strftime('%b %d, %H:%M')
                ))

            # --- E. INJECT BASELINE ---
            if not snap_base.empty:
                b_temps = snap_base['temperature']
                if unit_mode == "Celsius": b_temps = (b_temps - 32) * 5/9
                
                fig.add_trace(go.Scatter(
                    x=b_temps, y=snap_base['Depth_Num'], 
                    mode='lines+markers', 
                    name=f'<b>Baseline ({baseline_date_str})</b>',
                    line=dict(color='black', width=3, dash='dash'),
                    marker=dict(size=5, color='black'),
                    hovertemplate=f"Baseline: {baseline_date_str}<br>Depth: %{{y}}ft<br>Temp: %{{x:.1f}}{unit_label}<extra></extra>"
                ))

            fig.add_vline(x=freeze_pt, line_width=2, line_dash="solid", line_color="#ADD8E6")

            max_depth = loc_data['Depth_Num'].max()
            y_limit = int(((max_depth // 10) + 1) * 10) if pd.notnull(max_depth) else 50

            # THE FIX 2: Added explicit padding margins (l, r, t, b) so the right mirror border isn't cut off
            fig.update_layout(
                title=f"<b>Temp vs Depth - {loc}</b>",
                plot_bgcolor='white', 
                height=800,
                margin=dict(l=60, r=40, t=80, b=80), 
                xaxis=dict(
                    title=f"Temperature ({unit_label})", 
                    range=[-20, 80], dtick=10,
                    minor=dict(dtick=2, showgrid=True, gridcolor='#f8f8f8'),
                    gridcolor='Gainsboro', showline=True, linewidth=2, linecolor='black', mirror=True
                ),
                yaxis=dict(
                    title="Depth (ft)", 
                    range=[y_limit, 0], dtick=10,
                    minor=dict(dtick=2, showgrid=True, gridcolor='#f8f8f8'),
                    gridcolor='Silver', showline=True, linewidth=2, linecolor='black', mirror=True
                ),
                legend=dict(orientation="h", y=-0.1, xanchor="center", x=0.5)
            )
            
            st.plotly_chart(fig, use_container_width=True, key=f"depth_cht_{selected_project}_{loc}")
