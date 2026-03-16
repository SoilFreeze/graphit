# --- TAB: 24-HOUR INSIGHTS ---
with tab_summary:
    col1, col2 = st.columns([2, 1])
    now = pd.Timestamp.now(tz='UTC')
    last_24 = df_proj[df_proj['timestamp'] >= (now - pd.Timedelta(hours=24))].copy()
    
    with col1:
        if not last_24.empty:
            node_stats = last_24.groupby(['location', 'depth'])['value'].agg(['min', 'max']).reset_index()
            node_stats['delta'] = node_stats['max'] - node_stats['min']
            
            pipe_rows = []
            bank_rows = []
            
            for loc in sorted(last_24['location'].unique()):
                pipe_data = node_stats[node_stats['location'] == loc]
                p_min, p_max = pipe_data['min'].min(), pipe_data['max'].max()
                top_node_row = pipe_data.loc[pipe_data['delta'].idxmax()]
                
                row = {
                    "Pipe": loc,
                    "Min Temp": f"{p_min:.1f}{u_symbol}",
                    "Max Temp": f"{p_max:.1f}{u_symbol}",
                    "Max Change at": top_node_row['depth'],
                    "Raw Delta": top_node_row['delta'], # Hidden helper for styling
                    "24h Change": f"{top_node_row['delta']:.1f}{u_symbol}"
                }
                
                if "bank" in loc.lower():
                    bank_rows.append(row)
                else:
                    row["Max Change at"] = f"{float(row['Max Change at']):.1f}ft"
                    pipe_rows.append(row)

            # Updated styling function to use the 'Raw Delta' helper
            def style_alert(row):
                color = 'red' if row['Raw Delta'] >= alert_threshold else None
                return [f'color: {color}' if color else '' for _ in row]

            # Display Standard Pipes
            st.subheader("Standard Pipes: 24h Activity")
            if pipe_rows:
                df_p = pd.DataFrame(pipe_rows)
                # Apply styling and THEN hide the helper column
                st.table(df_p.style.apply(style_alert, axis=1)
                         .hide(axis='columns', subset=['Raw Delta']))
            
            # Display Banks
            st.subheader("Bank Temperatures: 24h Activity")
            if bank_rows:
                df_b = pd.DataFrame(bank_rows)
                st.table(df_b.style.apply(style_alert, axis=1)
                         .hide(axis='columns', subset=['Raw Delta']))
        else:
            st.info("No active data in the last 24 hours.")
