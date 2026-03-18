# --- SERVICE: DATA CLEANING TOOL ---
elif service == "🧹 Data Cleaning Tool" and not full_df.empty:
    st.header("🧹 Surgical Data Cleaning")
    
    # 1. Selection Controls with Date Range
    c_col1, c_col2 = st.columns(2)
    with c_col1:
        clean_projs = sorted([p for p in full_df['Project'].unique() if p is not None])
        sel_c_proj = st.selectbox("Project to Clean", clean_projs)
    with c_col2:
        c_locs = ["All Locations"] + sorted([l for l in full_df[full_df['Project']==sel_c_proj]['Location'].unique() if l is not None])
        sel_c_loc = st.selectbox("Location Filter", c_locs)

    # Added Date Range for cleaning multiple days at once
    r_col1, r_col2 = st.columns(2)
    with r_col1:
        clean_start = st.date_input("Start Date", value=date.today() - timedelta(days=1))
    with r_col2:
        clean_end = st.date_input("End Date", value=date.today())

    # Filter for the cleaning chart using the range
    clean_view_df = full_df[
        (full_df['Project'] == sel_c_proj) & 
        (full_df['timestamp'].dt.date >= clean_start) & 
        (full_df['timestamp'].dt.date <= clean_end)
    ].copy()
    
    if sel_c_loc != "All Locations": 
        clean_view_df = clean_view_df[clean_view_df['Location'] == sel_c_loc]

    # 2. Interactive Chart
    st.subheader("Highlight 'Spikes' to Clean")
    fig_clean = px.scatter(
        clean_view_df, 
        x='timestamp', 
        y='value', 
        color='nodenumber', 
        range_y=[-40, 100],
        title="Use Box Select to capture bad data"
    )
    fig_clean.update_layout(dragmode='select', plot_bgcolor='white')
    selected_points = st.plotly_chart(fig_clean, width='stretch', on_select="rerun")

    # 3. Execution Panel (THE DELETE BUTTON)
    if selected_points and "points" in selected_points and len(selected_points["points"]) > 0:
        pts = pd.DataFrame(selected_points["points"])
        
        # Get unique timestamps and nodes from the selection
        target_times = pts['x'].unique().tolist()
        target_nodes = pts['customdata'].tolist() if 'customdata' in pts else [] # Fallback for node IDs

        st.error(f"⚠️ TARGETING {len(pts)} DATA POINTS")
        
        del_scope = st.radio(
            "Targeting Scope:",
            ["Delete only specific selected points", "Delete these timestamps for ALL nodes in project"],
            help="Site-wide deletion is best for power surges or radio interference affecting everyone."
        )
        
        # The Execute Button
        if st.button("🔥 PERMANENTLY DELETE SELECTED DATA"):
            # Create the SQL logic
            time_list = ", ".join([f"'{t}'" for t in target_times])
            
            if del_scope == "Delete only specific selected points":
                sql = f"DELETE FROM `sensor_data` WHERE Project = '{sel_c_proj}' AND timestamp IN ({time_list})"
            else:
                sql = f"DELETE FROM `sensor_data` WHERE Project = '{sel_c_proj}' AND timestamp IN ({time_list})"
            
            st.code(sql, language="sql")
            st.warning("Copy the SQL above into BigQuery to execute, or enable direct-delete in app settings.")
            
            # Placeholder for direct execution:
            # client.query(sql)
            # st.cache_data.clear() # Refresh data after delete
    else:
        st.info("👆 Use the 'Box Select' tool on the graph to highlight bad data.")
