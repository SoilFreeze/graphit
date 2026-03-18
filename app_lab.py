# --- 3. THE GRAPH (Standardized -20 to 80°F with Weekly Markers) ---
    if not plot_df.empty:
        # Create the base plot
        fig = px.line(
            plot_df, 
            x='timestamp', 
            y='value', 
            color='Sensor_ID',
            title=f"Site: {sel_proj} | Location: {sel_loc}",
            labels={'Sensor_ID': 'Sensor', 'value': 'Temp (°F)'},
            range_y=[-20, 80]
        )

        # 1. Identify all Monday Midnights in the current data range for the dark lines
        min_date = plot_df['timestamp'].min()
        max_date = plot_df['timestamp'].max()
        
        # Generate a list of Mondays between the start and end of your data
        mondays = pd.date_range(
            start=min_date.floor('D'), 
            end=max_date.ceil('D'), 
            freq='W-MON'
        )

        # 2. Add the Dark Vertical Lines for Monday Midnight
        for monday in mondays:
            fig.add_vline(
                x=monday.timestamp() * 1000, # Plotly expects milliseconds for timestamps
                line_width=2, 
                line_color="black", 
                line_dash="solid",
                opacity=0.8
            )

        # 3. Configure the X-Axis for Daily Grid Lines
        fig.update_xaxes(
            showgrid=True,
            dtick=86400000.0, # One day in milliseconds
            gridcolor='lightgrey',
            gridwidth=1,
            tickformat="%a\n%b %d" # Shows "Mon Nov 01" etc.
        )

        # 4. Final Layout Adjustments
        fig.update_layout(
            legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02),
            margin=dict(r=150),
            hovermode="x unified",
            plot_bgcolor='white' # Making background white makes the grid lines easier to see
        )
        
        fig.update_traces(connectgaps=True)
        fig.add_hline(y=32, line_dash="dash", line_color="blue", annotation_text="32°F")
        
        st.plotly_chart(fig, use_container_width=True)
