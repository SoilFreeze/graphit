# --- TAB: 24-HOUR INSIGHTS ---
with tab_summary:
    col1, col2 = st.columns([2, 1])
    now = pd.Timestamp.now(tz='UTC')
    last_24 = df_proj[df_proj['timestamp'] >= (now - pd.Timedelta(hours=24))].copy()
    
    with col1:
        if not last_24.empty:
            node_stats = last_24.groupby(['location', 'depth'])['value'].agg(['min', 'max']).reset_index()
            node_stats['delta'] = node_stats['max'] - node_stats['min']
            
            pipe_rows, bank_rows = [], []
            for loc in sorted(last_24['location'].unique()):
                pipe_data = node_stats[node_stats['location'] == loc]
                p_min, p_max = pipe_data['min'].min(), pipe_data['max'].max()
                top_node_row = pipe_data.loc[pipe_data['delta'].idxmax()]
                
                row = {
                    "Pipe": loc,
                    "Min Temp": f"{p_min:.1f}{u_symbol}",
                    "Max Temp": f"{p_max:.1f}{u_symbol}",
                    "Max Change at": top_node_row['depth'],
                    "Raw Delta": top_node_row['delta'], # Still used for logic...
                    "24h Change": f"{top_node_row['delta']:.1f}{u_symbol}"
                }
                
                if "bank" in loc.lower():
                    bank_rows.append(row)
                else:
                    row["Max Change at"] = f"{float(row['Max Change at']):.1f}ft"
                    pipe_rows.append(row)

            # Function to apply red text based on the Raw Delta value
            def apply_formatting(df):
                # Create a mask where the delta exceeds the threshold
                is_alert = df['Raw Delta'] >= alert_threshold
                # Drop the Raw Delta column so it literally doesn't exist in the display
                display_df = df.drop(columns=['Raw Delta'])
                
                # Apply the red color to the rows that hit the threshold
                return display_df.style.map(
                    lambda v: 'color: red;', 
                    subset=pd.IndexSlice[is_alert, :]
                )

            st.subheader("Standard Pipes: 24h Activity")
            if pipe_rows:
                st.table(apply_formatting(pd.DataFrame(pipe_rows)))
            
            st.subheader("Bank Temperatures: 24h Activity")
            if bank_rows:
                st.table(apply_formatting(pd.DataFrame(bank_rows)))
        else:
            st.info("No active data in the last 24 hours.")
