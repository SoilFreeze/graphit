# --- TAB: 24-HOUR INSIGHTS ---
with tab_summary:
    col1, col2 = st.columns([2, 1])
    now = pd.Timestamp.now(tz='UTC')
    last_24 = df_proj[df_proj['timestamp'] >= (now - pd.Timedelta(hours=24))].copy()
    
    with col1:
        if not last_24.empty:
            node_stats = last_24.groupby(['location', 'depth'])['value'].agg(['min', 'max']).reset_index()
            node_stats['delta'] = node_stats['max'] - node_stats['min']
            
            p_rows, b_rows = [], []
            for loc in sorted(last_24['location'].unique()):
                pipe_data = node_stats[node_stats['location'] == loc]
                p_min, p_max = pipe_data['min'].min(), pipe_data['max'].max()
                top_node = pipe_data.loc[pipe_data['delta'].idxmax()]
                
                # We store the numeric delta in a dictionary for logic, 
                # but we only put the string version in the final list.
                row_data = {
                    "Pipe": loc,
                    "Min Temp": f"{p_min:.1f}{u_symbol}",
                    "Max Temp": f"{p_max:.1f}{u_symbol}",
                    "Max Change at": f"{float(top_node['depth']):.1f}ft" if "bank" not in loc.lower() else top_node['depth'],
                    "24h Change": f"{top_node['delta']:.1f}{u_symbol}"
                }
                
                # Logic: apply a style if the delta exceeds the threshold
                style_color = 'color: red' if top_node['delta'] >= alert_threshold else ''
                
                if "bank" in loc.lower():
                    b_rows.append((row_data, style_color))
                else:
                    p_rows.append((row_data, style_color))

            def render_custom_table(rows, title):
                st.subheader(title)
                if rows:
                    # Convert to DF and apply row-based styling using the pre-calculated color
                    df = pd.DataFrame([r[0] for r in rows])
                    colors = [r[1] for r in rows]
                    
                    def apply_row_styles(x):
                        # Create a DataFrame of empty strings with the same shape
                        style_df = pd.DataFrame('', index=x.index, columns=x.columns)
                        for i, color in enumerate(colors):
                            style_df.iloc[i, :] = color
                        return style_df

                    st.table(df.style.apply(apply_row_styles, axis=None))

            render_custom_table(p_rows, "Standard Pipes: 24h Activity")
            render_custom_table(b_rows, "Bank Temperatures: 24h Activity")
        else:
            st.info("No data found for the last 24 hours.")

    with col2:
        st.subheader("⚠️ Offline Sensors")
        # Logic to find sensors that exist in the project but haven't reported in 24h
        all_sens = df_proj[['location', 'depth']].drop_duplicates()
        act_sens = last_24[['location', 'depth']].drop_duplicates()
        
        # Merge to find the difference
        offline = all_sens.merge(act_sens, on=['location', 'depth'], how='left', indicator=True)
        offline = offline[offline['_merge'] == 'left_only'].copy()
        
        if not offline.empty:
            st.warning(f"{len(offline)} nodes offline (24h+)")
            # Clean up the display for the offline table
            off_display = offline[['location', 'depth']].rename(columns={'location': 'Pipe', 'depth': 'Node'})
            st.dataframe(off_display, hide_index=True)
        else:
            st.success("All project nodes are online.")
