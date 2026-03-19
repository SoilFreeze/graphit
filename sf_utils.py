import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import math
import streamlit as st

def get_standard_24h_summary(df, theme):
    """
    STANDARDIZED 24-HOUR PERFORMANCE TABLE
    Input: Project DataFrame, Theme JSON
    Output: Styled Styler object for Streamlit
    """
    now = pd.Timestamp.now(tz='UTC')
    last_24 = df[df['timestamp'] >= (now - pd.Timedelta(hours=24))].copy()
    
    if last_24.empty:
        return None

    node_analysis = []
    for node in last_24['nodenumber'].unique():
        n_df = last_24[last_24['nodenumber'] == node].sort_values('timestamp')
        if len(n_df) > 1:
            change = n_df['value'].iloc[-1] - n_df['value'].iloc[0]
            node_analysis.append({
                "Depth": n_df['Depth'].iloc[0],
                "Location": n_df['Location'].iloc[0],
                "Min Temp": n_df['value'].min(),
                "Max Temp": n_df['value'].max(),
                "Current": n_df['value'].iloc[-1],
                "24h Change": change
            })

    summary_table = pd.DataFrame(node_analysis).sort_values(['Location', 'Depth'])
    
    # Apply Standard Highlighting from JSON
    def style_logic(row):
        val = row['24h Change']
        t = theme['table_theme']['thresholds']
        c = theme['table_theme']['status_colors']
        
        if val >= t['critical_warming']: return [f'background-color: {c["offline_red"]}; color: white'] * len(row)
        elif val >= t['warning_warming']: return [f'background-color: {c["warning_orange"]}; color: black'] * len(row)
        elif val >= t['slight_warming']: return [f'background-color: {c["standby_yellow"]}; color: black'] * len(row)
        elif val <= t['cooling']: return [f'background-color: {c["healthy_green"]}; color: black'] * len(row)
        return [''] * len(row)

    return summary_table.style.apply(style_logic, axis=1).format({
        "Min Temp": "{:.2f}°F", "Max Temp": "{:.2f}°F",
        "Current": "{:.2f}°F", "24h Change": "{:+.2f}°F"
    })

def apply_standard_chart_style(fig, theme, is_profile=False):
    """
    STANDARDIZED CHART STYLER
    Ensures the 'Boxed' look and standard gridlines across all apps.
    """
    c = theme['chart_theme']['colors']
    d = theme['chart_theme']['dimensions']
    
    fig.update_layout(
        plot_bgcolor=c['plot_background'],
        height=d['default_height'],
        margin=dict(l=d['margin_left'], r=50, t=50, b=50),
        hovermode="y unified"
    )

    # Standard X-Axis (Temperature)
    fig.update_xaxes(
        gridcolor=c['grid_major_20s'],
        gridwidth=d['grid_width_major'],
        minor=dict(dtick=5, gridcolor=c['grid_minor_5s'], showgrid=True),
        mirror=True, showline=True, 
        linecolor=c['frame_border'], linewidth=d['frame_width']
    )

    if is_profile:
        fig.update_yaxes(
            autorange="reversed",
            gridcolor=c['grid_major_20s'],
            minor=dict(dtick=1, gridcolor=c['grid_faint_1s'], showgrid=True),
            mirror=True, showline=True, 
            linecolor=c['frame_border'], linewidth=d['frame_width']
        )
        
    # Standard Reference Lines
    fig.add_vline(x=32, line_dash="dash", line_color=c['ref_32_freezing'], annotation_text="32°F")
    fig.add_vline(x=26.6, line_color=c['ref_26_6_target'], annotation_text="26.6°F")
    fig.add_vline(x=10.2, line_color=c['ref_10_2_alert'], annotation_text="10.2°F")
    
    return fig
