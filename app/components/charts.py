import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import re
import app.utils.config as cfg
from app.data.processor import get_bq_client # Import the shared connection

def natural_sort_key(text):
    return [int(c) if c.isdigit() else str(c).lower() for c in re.split(r'(\d+)', str(text))]

@st.cache_data(ttl=600, show_spinner=False)
def get_cached_reference_curve(curve_id, loc_digit):
    """Fetches reference curves once and caches them for all charts."""
    client = get_bq_client()
    if client is None: return pd.DataFrame()
    
    parts = str(curve_id).split('-')
    proj_num = parts[0].strip() if len(parts) > 0 else ""
    target_q = f"""
        SELECT CurveID, Day, Temp 
        FROM `{cfg.PROJECT_ID}.{cfg.DATASET_ID}.reference_curves` 
        WHERE CurveID LIKE '%{proj_num}%' 
        AND REGEXP_CONTAINS(CurveID, r'[T|TP]0?{loc_digit}([^0-9]|$)')
        AND NOT REGEXP_CONTAINS(CurveID, r'(?i)brine')
        ORDER BY Day
    """
    try:
        return client.query(target_q).to_dataframe()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=600, show_spinner=False)
def get_cached_ambient_data(job_num, start_str):
    """Fetches ambient data once and caches it for all charts."""
    client = get_bq_client()
    if client is None: return pd.DataFrame()
    
    amb_q = f"""
        SELECT NodeNum, timestamp, temperature 
        FROM `{cfg.PROJECT_ID}.{cfg.DATASET_ID}.master_data_view_v2` 
        WHERE Project LIKE '{job_num}%' 
          AND UPPER(Location) = 'AMBIENT'
          AND timestamp >= '{start_str}'
    """
    try:
        return client.query(amb_q).to_dataframe()
    except:
        return pd.DataFrame()

# THE FIX: Added 'client' as the first argument
def build_high_speed_graph(client, df, title, start_view, end_view, active_refs, unit_mode, unit_label, 
                           display_tz="UTC", mobile_mode=False, f_start_date=None, curve_id=None):
    """
    Engineering-grade Trend Graph.
    """
    clean_title_lower = str(title).lower().replace("thermal trends:", "").strip()
    
    if any(x in clean_title_lower for x in ['ambient', 'office', 'x-tra', 'xtra']):
        return None
        
    if df.empty: return go.Figure().update_layout(title="No data available")

    # THE FIX: Removed the get_bq_client() call here! We use the 'client' passed into the function.
    plot_df = df.copy() 

    # 1. TIMEZONE & UNITS
    if plot_df['timestamp'].dt.tz is None:
        plot_df['timestamp'] = plot_df['timestamp'].dt.tz_localize('UTC')
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(display_tz)
    
    freeze_pt = 0 if unit_mode == "Celsius" else 32
    y_range = [-30, 30] if unit_mode == "Celsius" else [-20, 80]

    fig = go.Figure()
    final_end_view, final_start_view = end_view, start_view

    is_temp_pipe = any(x in clean_title_lower for x in ['pipe', 'tp', 'depth']) or clean_title_lower.startswith('t')
    
    if curve_id and curve_id != "None" and f_start_date and is_temp_pipe and st.session_state.get('global_show_ref', True):
        try:
            parts = str(curve_id).split('-')
            proj_num = parts[0].strip() if len(parts) > 0 else ""
            
            # Extract the pipe number from the graph's clean title
            digits = re.findall(r'\d+', clean_title_lower)
            loc_digit = digits[0] if digits else ""
            
            target_q = f"""
                SELECT CurveID, Day, Temp 
                FROM `{cfg.PROJECT_ID}.{cfg.DATASET_ID}.reference_curves` 
                WHERE CurveID LIKE '%{proj_num}%' 
                AND REGEXP_CONTAINS(CurveID, r'[T|TP]0?{loc_digit}([^0-9]|$)')
                AND NOT REGEXP_CONTAINS(CurveID, r'(?i)brine')
                ORDER BY Day
            """
            target_df = get_cached_reference_curve(curve_id, loc_digit)
            
            if not target_df.empty:
                dash_styles = ['dashdot', 'dash', 'dot']
                gray_shades = ['rgba(30,30,30,0.8)', 'rgba(70,70,70,0.75)', 'rgba(110,110,110,0.7)']
                
                for c_idx, (cid, c_df) in enumerate(target_df.groupby('CurveID')):
                    c_df = c_df.copy()
                    c_df['timestamp'] = c_df['Day'].apply(lambda d: pd.Timestamp(f_start_date) + pd.Timedelta(days=d))
                    c_df['timestamp'] = c_df['timestamp'].dt.tz_localize('UTC').dt.tz_convert(display_tz)
                    ref_y = c_df['Temp'] if unit_mode == "Fahrenheit" else (c_df['Temp'] - 32) * 5/9
                    
                    # --- EXTEND X-AXIS TO ACCOMMODATE THE FULL CURVE ---
                    curve_max_ts = c_df['timestamp'].max()
                    
                    if curve_max_ts.tzinfo is not None:
                        curve_max_ts = curve_max_ts.tz_localize(None)
                        
                    if curve_max_ts > final_end_view:
                        final_end_view = curve_max_ts
                    
                    # FASTER RENDERING: Changed to Scattergl
                    fig.add_trace(go.Scattergl(
                        x=c_df['timestamp'], y=ref_y, name=f"<b>Goal: {cid}</b>", 
                        mode='lines',
                        line=dict(color=gray_shades[c_idx % len(gray_shades)], width=3.5, dash=dash_styles[c_idx % len(dash_styles)], shape='spline', smoothing=1.3),
                        legendrank=1 
                    ))
        except:
            pass # Fail silently
                
    # 3. SENSOR DATA (Prioritized & Filtered)
    sf_15_palette = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf', '#FF1493', '#00CED1', '#FFD700', '#8A2BE2', '#32CD32']
    
    node_metadata = []
    skip_keywords = ['AMBIENT', 'OFFICE', 'X-TRA', 'XTRA']
    empty_vals = ['nan', 'none', '', '—', '-']

    if is_temp_pipe:
        plot_df['Logical_Position'] = plot_df['Depth'].astype(str).str.strip()
    else:
        plot_df['Logical_Position'] = plot_df['Bank'].astype(str).str.strip()

    plot_df = plot_df[~plot_df['Logical_Position'].str.lower().isin(empty_vals)]

    for pos in plot_df['Logical_Position'].unique():
        if any(x in pos.upper() for x in skip_keywords):
            continue

        pos_df = plot_df[plot_df['Logical_Position'] == pos].sort_values('timestamp')
        
        if pos_df.empty:
            continue
            
        latest_sensor = str(pos_df.iloc[-1]['NodeNum']).strip()

        if is_temp_pipe:
            display_name = f"{pos} ft ({latest_sensor})"
            priority = 1
            try: sort_val = [float(pos)]
            except: sort_val = natural_sort_key(pos)
        else:
            display_name = f"{pos} ({latest_sensor})"
            priority = 0
            sort_val = natural_sort_key(pos)

        node_metadata.append({
            'position': pos, 
            'display_name': display_name, 
            'priority': priority, 
            'sort_key': sort_val
        })

    sorted_node_configs = sorted(node_metadata, key=lambda x: (x['priority'], x['sort_key']))

    for i, node_cfg in enumerate(sorted_node_configs):
        pos = node_cfg['position']
        display_name = node_cfg['display_name']
        
        s_df = plot_df[plot_df['Logical_Position'] == pos].sort_values('timestamp')
        s_df = s_df.set_index('timestamp').resample('1h').first().reset_index()
        s_df = s_df.dropna(subset=['temperature']).copy()
        
        s_df['time_diff'] = s_df['timestamp'].diff()
        gap_mask = s_df['time_diff'] > pd.Timedelta(hours=6)
        
        if gap_mask.any():
            gap_rows = s_df[gap_mask].copy()
            gap_rows['timestamp'] = gap_rows['timestamp'] - pd.Timedelta(seconds=1)
            gap_rows['temperature'] = float('nan')
            s_df = pd.concat([s_df, gap_rows]).sort_values('timestamp')
        
        # FASTER RENDERING: Changed to Scattergl
        fig.add_trace(go.Scattergl(
            x=s_df['timestamp'], 
            y=s_df['temperature'],
            name=display_name, 
            mode='lines',
            connectgaps=False, 
            customdata=s_df[['NodeNum']], 
            line=dict(shape='spline', smoothing=1.3, width=2, color=sf_15_palette[i % 15]),
            hovertemplate="<b>%{fullData.name}</b>: %{y:.1f}" + unit_label + " <i>(Node: %{customdata[0]})</i><extra></extra>"
        ))
        
        if 'approval_status' in s_df.columns:
            s_status = s_df['approval_status'].fillna('TRUE').astype(str).str.replace(' ', '').str.upper().str.strip()
            
            if st.session_state.get('global_show_masked', False):
                masked_df = s_df[s_status == 'MASKED']
                if not masked_df.empty:
                    # FASTER RENDERING: Changed to Scattergl
                    fig.add_trace(go.Scattergl(
                        x=masked_df['timestamp'], y=masked_df['temperature'],
                        name=display_name + " [MASKED]", mode='markers',
                        customdata=masked_df[['NodeNum']],
                        marker=dict(symbol='circle-open', size=9, color='orange', line=dict(width=2.5)),
                        hovertemplate="<b>⚠️ MASKED</b> <i>(%{customdata[0]})</i><br>Temp: %{y:.1f}" + unit_label + "<extra></extra>",
                        showlegend=False
                    ))
            
            if st.session_state.get('global_show_baddata', False):
                bad_df = s_df[s_status == 'BADDATA']
                if not bad_df.empty:
                    # FASTER RENDERING: Changed to Scattergl
                    fig.add_trace(go.Scattergl(
                        x=bad_df['timestamp'], y=bad_df['temperature'],
                        name=display_name + " [BAD]", mode='markers',
                        customdata=bad_df[['NodeNum']],
                        marker=dict(symbol='x', size=9, color='red', line=dict(width=2.5)),
                        hovertemplate="<b>❌ BAD DATA</b> | %{y:.1f}" + unit_label + " <i>(Node: %{customdata[0]})</i><extra></extra>",
                        showlegend=False
                    ))
        
    is_brine_graph = not is_temp_pipe
    
    if st.session_state.get('global_show_ambient', True) and is_brine_graph:
        p_name = ""
        if 'Project' in plot_df.columns and not plot_df.empty:
            p_name = str(plot_df['Project'].iloc[0])
        elif 'Raw_Project_Name' in plot_df.columns and not plot_df.empty:
            p_name = str(plot_df['Raw_Project_Name'].iloc[0])
        else:
            p_name = st.session_state.get('selected_project', '')
            
        job_num = p_name.split('-')[0].strip()
        
        if job_num:
            start_str = pd.to_datetime(start_view).strftime('%Y-%m-%d %H:%M:%S')
            amb_q = f"""
                SELECT NodeNum, timestamp, temperature 
                FROM `{cfg.PROJECT_ID}.{cfg.DATASET_ID}.master_data_view_v2` 
                WHERE Project LIKE '{job_num}%' 
                  AND UPPER(Location) = 'AMBIENT'
                  AND timestamp >= '{start_str}'
            """
            try:
                amb_df = get_cached_ambient_data(job_num, start_str)
                if not amb_df.empty:
                    if amb_df['timestamp'].dt.tz is None:
                        amb_df['timestamp'] = amb_df['timestamp'].dt.tz_localize('UTC')
                    amb_df['timestamp'] = amb_df['timestamp'].dt.tz_convert(display_tz)
                    
                    amb_df = amb_df.set_index('timestamp').resample('1h')['temperature'].mean().dropna().reset_index()
                    
                    # FASTER RENDERING: Changed to Scattergl
                    fig.add_trace(go.Scattergl(
                        x=amb_df['timestamp'], y=amb_df['temperature'],
                        name="Ambient Air (Site Avg)", mode='lines',
                        connectgaps=False,
                        line=dict(width=2.5, dash='dot', color='orange'),
                        hovertemplate="<b>Site Ambient Avg</b><br>Time: %{x|%H:%M}<br>Temp: %{y:.1f}" + unit_label + "<extra></extra>",
                        legendrank=99 
                    ))
            except Exception:
                pass

    fig.add_hline(y=freeze_pt, line_width=2, line_dash="dash", line_color="RoyalBlue", annotation_text="32°F FREEZE", layer="above")
    
    now_ts = pd.Timestamp.now(tz=display_tz)
    fig.add_vline(x=now_ts.to_pydatetime(), line_width=2, line_color="red", line_dash="dash", layer='above')
    
    m_range = pd.date_range(start=final_start_view, end=final_end_view, freq='W-MON')
    for m_dt in m_range:
        fig.add_vline(x=m_dt, line_width=1.5, line_color="black", opacity=0.4)

    if 'Project' in plot_df.columns and not plot_df.empty:
        p_name = str(plot_df['Project'].iloc[0])
    elif 'Raw_Project_Name' in plot_df.columns and not plot_df.empty:
        p_name = str(plot_df['Raw_Project_Name'].iloc[0])
    else:
        p_name = st.session_state.get('selected_project', 'Unknown Project')
    
    clean_title = str(title).replace("Thermal Trends:", "").strip()
    
    if is_temp_pipe:
        header_text = f"Time vs Temperature - Temperatures for Temperature Pipe {clean_title}"
    else:
        header_text = f"Time vs Temperature - Temperatures for Brine Bank {clean_title}"

    footer_annotations = [
        dict(
            x=0.02, y=-0.12, 
            xref='paper', yref='paper',
            text=f"<b>Project:</b> {p_name}",
            showarrow=False, xanchor='left', yanchor='top',
            font=dict(size=13, color="#666")
        ),
        dict(
            x=0.98, y=-0.12,
            xref='paper', yref='paper',
            text=f"<b>Type:</b> Time vs Temperature",
            showarrow=False, xanchor='right', yanchor='top',
            font=dict(size=13, color="#666")
        )
    ]

    fig.update_layout(
        title=dict(text=f"<b>{header_text}</b>", x=0.5, xanchor='center', y=0.96, font=dict(size=19)),
        plot_bgcolor='white', hovermode="x unified", height=680,
        margin=dict(l=60, r=40, t=80, b=120), 
        annotations=footer_annotations,
        xaxis=dict(range=[final_start_view, final_end_view], showgrid=True, gridcolor='Gainsboro', showline=True, mirror=True, linecolor='black', linewidth=2, hoverformat='%A, %b %d, %Y', tickformat='%b %d', minor=dict(dtick=1000*60*60*24, showgrid=True, gridcolor='#f8f8f8')),
        yaxis=dict(title=f"Temperature ({unit_label})", range=y_range, dtick=10, showgrid=True, gridcolor='Gainsboro', showline=True, mirror=True, linecolor='black', linewidth=2, minor=dict(dtick=2, showgrid=True, gridcolor='#f8f8f8')),
        legend=dict(orientation="v", x=1.02, y=1, xanchor="left", yanchor="top"),
    )
    fig.update_xaxes(hoverformat="%b %d, %Y %I:%M %p")                           
    return fig
                               
def get_soil_reference_curves(soil_type, start_date, unit_mode):
    """
    Fallback function for hardcoded soil types.
    """
    references = {
        "Silty Sand": [(0, 50), (5, 32), (14, 20), (30, 10), (60, 5)],
        "Clay":       [(0, 50), (10, 32), (25, 25), (45, 15), (90, 10)]
    }
    
    curve = references.get(soil_type, [])
    if not curve: return None, None
        
    x_times = [pd.Timestamp(start_date) + pd.Timedelta(days=d) for d, t in curve]
    y_temps = [t if unit_mode == "Fahrenheit" else (t - 32) * 5/9 for d, t in curve]
    return x_times, y_temps



