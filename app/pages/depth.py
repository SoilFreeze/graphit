import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import re

# You will need your custom functions from your internal modules
from app.data.processor import get_universal_portal_data
from app.pages.admin import natural_sort_key


#########################
# Page 3 - Depth Charts #
#########################
def render_depth_charts(selected_project, unit_label, display_tz, orientation="vertical"):
    is_horizontal = str(orientation).strip().lower() == "horizontal"
    chart_type_label = "Distance" if is_horizontal else "Depth"
    """
    Vertical Temperature Profiles.
    Maps arrays dynamically based on native view Depth allocations.
    """
    st.header(f"📏 {chart_type_label} Profile Analysis: {selected_project}")
    
    if not selected_project or selected_project == "All Projects":
        st.info(f"💡 Please select a specific project in the sidebar to view {chart_type_label.lower()} profiles.")
        return

    # --- FIX: SEPARATE CHART DISPLAY WINDOW FROM DATA FETCH WINDOW ---
    
    # 1. Read the slider to determine how many lines to draw on the chart
    lookback_weeks = st.session_state.get("global_lookback_weeks_slider", 5)
    st.sidebar.caption(f"📏 {chart_type_label} Charts displaying the last {lookback_weeks} weeks.")
    
    # 2. Calculate the exact days back to Freezedown to ensure we capture the True Baseline
    p_meta = st.session_state.get('project_metadata') or {}
    real_f_date = p_meta.get('Date_Freezedown')
    now_utc = pd.Timestamp.now(tz='UTC')
    
    # The minimum data we need to fetch is whatever the slider is set to
    fetch_days = lookback_weeks * 7 
    
    if pd.notnull(real_f_date):
        parsed_f_date = pd.to_datetime(real_f_date)
        if parsed_f_date.tzinfo is None:
            parsed_f_date = parsed_f_date.tz_localize('UTC')
        
        days_since_freeze = (now_utc - parsed_f_date).days
        # If freezedown was 200 days ago, force BigQuery to fetch 200 days of data
        if days_since_freeze > fetch_days:
            fetch_days = days_since_freeze + 2

    with st.spinner("Fetching historical telemetry..."):
        # 3. Pass fetch_days into the portal function so it doesn't default to a small window
        try:
            p_df = get_universal_portal_data(
                selected_project, 
                lookback_days=fetch_days,
                show_masked=st.session_state.get('global_show_masked', False),
                show_baddata=st.session_state.get('global_show_baddata', False)
            )
        except TypeError:
            # Fallback just in case the processor doesn't accept the new arguments
            p_df = get_universal_portal_data(selected_project)
            
    if p_df is None or p_df.empty:
        st.warning("No data found for this project.")
        return

    # --- AUTO-FILTER BY PHASE FROM PROJECT TITLE ---
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
        with st.expander(f"📍 Temp vs {chart_type_label} - {loc}", expanded=True):
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
            for m_date in mondays:
                target_ts = m_date.replace(hour=6, minute=0, second=0)
                current_loop_date = target_ts.strftime('%Y-%m-%d')
                
                if current_loop_date == baseline_date_str or current_loop_date == recent_6am_date_str:
                    continue
                    
                window = loc_data[
                    (loc_data['timestamp_local'] >= target_ts - pd.Timedelta(hours=12)) & 
                    (loc_data['timestamp_local'] <= target_ts + pd.Timedelta(hours=12))
                ]
                
                if not window.empty:
                    snap_week = (
                        window.assign(diff=(window['timestamp_local'] - target_ts).abs())
                        .sort_values(['NodeNum', 'diff'])
                        .drop_duplicates('NodeNum')
                        .sort_values('Depth_Num')
                    )
                    
                    temps = snap_week['temperature']
                    if unit_mode == "Celsius": temps = (temps - 32) * 5/9
                    
                    # DYNAMIC AXES FLIP
                    x_week = snap_week['Depth_Num'] if is_horizontal else temps
                    y_week = temps if is_horizontal else snap_week['Depth_Num']
                    ht_week = f"Date: {current_loop_date}<br>{chart_type_label}: %{{{'x' if is_horizontal else 'y'}}}ft<br>Temp: %{{{'y' if is_horizontal else 'x'}:.1f}}{unit_label}<extra></extra>"

                    fig.add_trace(go.Scatter(
                        x=x_week, y=y_week, 
                        mode='lines+markers', 
                        name=current_loop_date,
                        line=dict(shape='spline', smoothing=1.1, width=1.5),
                        marker=dict(size=4),
                        hovertemplate=ht_week
                    ))

            # --- D. INJECT THE MOST RECENT LINE ---
            if not snap_recent.empty:
                recent_temps = snap_recent['temperature']
                if unit_mode == "Celsius": recent_temps = (recent_temps - 32) * 5/9
                
                # DYNAMIC AXES FLIP
                x_rec = snap_recent['Depth_Num'] if is_horizontal else recent_temps
                y_rec = recent_temps if is_horizontal else snap_recent['Depth_Num']
                ht_rec = f"Most Recent: %{{text}}<br>{chart_type_label}: %{{{'x' if is_horizontal else 'y'}}}ft<br>Temp: %{{{'y' if is_horizontal else 'x'}:.1f}}{unit_label}<extra></extra>"

                fig.add_trace(go.Scatter(
                    x=x_rec, y=y_rec,
                    mode='lines+markers',
                    name=f'<b>Most Recent ({recent_6am_date_str} 6AM*)</b>',
                    line=dict(color='#ff7f0e', width=3.5, shape='spline', smoothing=1.1),
                    marker=dict(size=6, color='#ff7f0e'),
                    hovertemplate=ht_rec,
                    text=snap_recent['timestamp_local'].dt.strftime('%b %d, %H:%M')
                ))

            # --- E. INJECT BASELINE ---
            if not snap_base.empty:
                b_temps = snap_base['temperature']
                if unit_mode == "Celsius": b_temps = (b_temps - 32) * 5/9
                
                # DYNAMIC AXES FLIP
                x_base = snap_base['Depth_Num'] if is_horizontal else b_temps
                y_base = b_temps if is_horizontal else snap_base['Depth_Num']
                ht_base = f"Baseline: {baseline_date_str}<br>{chart_type_label}: %{{{'x' if is_horizontal else 'y'}}}ft<br>Temp: %{{{'y' if is_horizontal else 'x'}:.1f}}{unit_label}<extra></extra>"

                fig.add_trace(go.Scatter(
                    x=x_base, y=y_base, 
                    mode='lines+markers', 
                    name=f'<b>Baseline ({baseline_date_str})</b>',
                    line=dict(color='black', width=3, dash='dash'),
                    marker=dict(size=5, color='black'),
                    hovertemplate=ht_base
                ))

            # FLIP THRESHOLD LINE
            if is_horizontal:
                fig.add_hline(y=freeze_pt, line_width=2, line_dash="solid", line_color="#ADD8E6")
            else:
                fig.add_vline(x=freeze_pt, line_width=2, line_dash="solid", line_color="#ADD8E6")

            max_depth = loc_data['Depth_Num'].max()
            y_limit = int(((max_depth // 10) + 1) * 10) if pd.notnull(max_depth) else 50

            # DYNAMIC LAYOUT DICTIONARIES
            dist_axis = dict(
                title=f"{chart_type_label} (ft)", 
                range=[0, y_limit] if is_horizontal else [y_limit, 0], 
                dtick=10,
                minor=dict(dtick=2, showgrid=True, gridcolor='#f8f8f8'),
                gridcolor='Silver', showline=True, linewidth=2, linecolor='black', mirror=True
            )
            temp_axis = dict(
                title=f"Temperature ({unit_label})", 
                range=[-20, 80], dtick=10,
                minor=dict(dtick=2, showgrid=True, gridcolor='#f8f8f8'),
                gridcolor='Gainsboro', showline=True, linewidth=2, linecolor='black', mirror=True
            )

            fig.update_layout(
                title=f"<b>Temp vs {chart_type_label} - {loc}</b>",
                plot_bgcolor='white', 
                height=800,
                margin=dict(l=60, r=40, t=80, b=80), 
                xaxis=dist_axis if is_horizontal else temp_axis,
                yaxis=temp_axis if is_horizontal else dist_axis,
                legend=dict(orientation="h", y=-0.1, xanchor="center", x=0.5)
            )
            
            st.plotly_chart(fig, use_container_width=True, key=f"depth_cht_{selected_project}_{loc}")
