import streamlit as st
import pandas as pd
import requests
from typing import Dict, Any, Optional, Tuple
import json
import io


from .chart_intent_detector import ChartIntentDetector, ChartIntent
from .chart_config import ChartConfigBuilder, ChartConfig
from .chart_generator import PlotlyChartGenerator

class ChatChartIntegration:
    def __init__(self, fastapi_base_url: str = None):
        """
        Initialize chat chart integration

        Args:
            fastapi_base_url: Base URL for your FastAPI service (e.g., "http://localhost:8000")
        """
        self.fastapi_base_url = fastapi_base_url or "http://localhost:8000"
        self.intent_detector = ChartIntentDetector()
        self.config_builder = ChartConfigBuilder()
        self.chart_generator = PlotlyChartGenerator()

        # Initialize session state for chart history
        if 'chart_history' not in st.session_state:
            st.session_state.chart_history = []

    def process_user_message(self, sql_query: str, user_role: str = None, original_message: str = "") -> Tuple[bool, str, Optional[Any]]:
        """
        Process SQL query and generate chart based on user intent
        """
        try:
            import asyncio
            data, columns = asyncio.run(self._fetch_data_from_api(sql_query, user_role))
            
            if data is None or data.empty:
                return True, "I couldn't find any data to create a chart. Please try a different query.", None
            
            print("ORIGINAL DATA:")
            print(f"Columns: {columns}")
            print(f"Data types: {data.dtypes.to_dict()}")
            print(f"Sample: {data.head(2).to_dict()}")
            
            # Step 3: Detect chart type
            chart_type = self._auto_detect_chart_type(data, columns, original_message)
            print("DETECTED CHART TYPE:", chart_type)
            
            # Step 4: Prepare data for the specific chart type
            prepared_data = self._prepare_data_for_chart(data, chart_type)
            
            if prepared_data.empty:
                return True, "No valid data available for chart creation after data preparation.", None
            
            # Step 5: Build chart configuration
            config = self._build_simple_chart_config(prepared_data, columns, chart_type, original_message)
            print(f"CHART CONFIG: {config.chart_type}, X:{config.x_column}, Y:{config.y_column}")
            
            # Step 6: Generate chart
            fig = self.chart_generator.generate_chart(config, prepared_data)
            
            # Step 7: Generate response
            response_text = f"I've created a {chart_type} chart with {len(prepared_data)} records."
            
            return True, response_text, fig
            
        except Exception as e:
            import traceback
            print("FULL ERROR:", traceback.format_exc())
            error_msg = f"Sorry, I encountered an error while creating the chart: {str(e)}"
            return True, error_msg, None


    def _convert_to_datetime(self, series: pd.Series) -> pd.Series:
        """Convert various date formats to datetime"""
        
        # If already datetime, return as is
        if pd.api.types.is_datetime64_any_dtype(series):
            return series
        
        # Try multiple date formats in order of likelihood
        date_formats = [
            '%Y%m%d',        # AS400 format: 20250805
            '%Y-%m-%d',      # ISO format: 2025-08-05
            '%d/%m/%Y',      # European: 05/08/2025
            '%m/%d/%Y',      # American: 08/05/2025
            '%d-%m-%Y',      # European dash: 05-08-2025
            '%m-%d-%Y',      # American dash: 08-05-2025
            '%Y/%m/%d',      # Alternative: 2025/08/05
            '%d.%m.%Y',      # German/European: 05.08.2025
            '%Y%m%d%H%M%S',  # With time: 20250805143022
            '%Y-%m-%d %H:%M:%S',  # ISO with time: 2025-08-05 14:30:22
        ]
        
        converted_series = None
        
        for fmt in date_formats:
            try:
                # Try to convert using this format
                converted_series = pd.to_datetime(series, format=fmt, errors='coerce')
                
                # If more than 80% converted successfully, use this format
                success_rate = converted_series.notna().sum() / len(series)
                if success_rate > 0.8:
                    print(f"Successfully converted dates using format: {fmt} (success rate: {success_rate:.1%})")
                    return converted_series
            except:
                continue
        
        # If no specific format worked, try pandas' automatic parsing
        try:
            converted_series = pd.to_datetime(series, errors='coerce')
            success_rate = converted_series.notna().sum() / len(series)
            if success_rate > 0.5:  # Lower threshold for auto-parsing
                print(f"Successfully auto-parsed dates (success rate: {success_rate:.1%})")
                return converted_series
        except:
            pass
        
        print(f"Failed to convert dates. Sample values: {series.head(3).tolist()}")
        return series  # Return original if all conversions fail

        
    def _auto_detect_chart_type(self, data: pd.DataFrame, columns: list, user_message: str = "") -> str:
        """Chart type detection based on user intent AND data structure"""
        
        # PRIORITY 1: Check user's explicit chart type request
        message_lower = user_message.lower()
        
        # Explicit pie chart requests
        if any(keyword in message_lower for keyword in ['pie chart', 'pie', 'percentage', 'proportion', 'distribution']):
            return 'pie'
        
        # Explicit bar chart requests  
        if any(keyword in message_lower for keyword in ['bar chart', 'bar', 'bars', 'column', 'compare']):
            return 'bar'
        
        # Explicit line chart requests
        if any(keyword in message_lower for keyword in ['line chart', 'line', 'trend', 'over time', 'time series']):
            return 'line'
        
        # PRIORITY 2: Auto-detect based on data structure (fallback)
        date_columns = [col for col in columns if 'date' in col.lower() or 'time' in col.lower()]
        if date_columns and len(columns) >= 2:
            return 'line'
        
        # Default to bar for comparisons
        return 'bar'


    def _build_simple_chart_config(self, data: pd.DataFrame, columns: list, chart_type: str, original_message: str = ""):
        """Build simple chart config from data with proper aggregation for pie charts"""
        
        from .chart_config import ChartConfig
        
        # Simple logic: first column as X, second as Y
        x_col = columns[0] if columns else 'category'
        y_col = columns[1] if len(columns) > 1 else columns[0] if columns else 'value'
        
        # FOR PIE CHARTS: Ensure we have aggregated data
        if chart_type == 'pie':
            # If we have multiple rows with same categories, aggregate them
            if len(data) > data[x_col].nunique():
                # Group by X column and sum Y column
                aggregation = 'sum'
            else:
                # Data is already aggregated
                aggregation = None
            title = f"{x_col.replace('_', ' ').title()} Distribution"
        else:
            aggregation = None
            title = f"{y_col.replace('_', ' ').title()} by {x_col.replace('_', ' ').title()}"
        
        return ChartConfig(
            chart_type=chart_type,
            title=title,
            x_column=x_col,
            y_column=y_col,
            color_column=None,
            aggregation=aggregation,  # This will trigger aggregation in chart generator
            chart_params={}
        )



    async def _fetch_data_from_api(self, sql_query: str, user_role: str = None, session_id: str = None) -> Tuple[Optional[pd.DataFrame], list]:
        """
        Fetch data using the same method as the working chat assistant
        """
        import asyncio
        import httpx
        import os
        
        try:
            # Use same authentication as working chat
            GATEWAY_TOKEN = os.getenv("GATEWAY_TOKEN")
            
            async with httpx.AsyncClient() as http_client:
                response = await http_client.post(
                    f"{self.fastapi_base_url}/api/execute-query",
                    json={"query": sql_query},
                    headers={
                        "Authorization": f"Bearer {GATEWAY_TOKEN}",
                        "X-Username": user_role or "harold"
                    },
                    timeout=30.0
                )
                
                result = response.json()
                print("API RESPONSE:", result)
                
                if result.get("success") and result.get("data") and result["data"].get("rows"):
                    df = pd.DataFrame(result["data"]["rows"])
                    return df, list(df.columns)
                else:
                    st.error(f"❗ No data found: {result.get('message', 'Unknown error')}")
                    return None, []
                    
        except Exception as e:
            st.error(f"❗ Data fetch error: {str(e)}")
            return None, []



    def _extract_data_from_response(self, api_response: dict, intent: ChartIntent) -> Optional[pd.DataFrame]:
        """
        Extract and format data from your API response for charting
        """

        # This depends on your API response format
        # You'll need to adapt this based on how your FastAPI returns data

        try:
            # Example: If your API returns data in a specific format
            if "data" in api_response:
                # Direct data format
                return pd.DataFrame(api_response["data"])

            elif "results" in api_response:
                # Results format
                return pd.DataFrame(api_response["results"])

            elif "response" in api_response:
                # If your API returns SQL results or structured data
                response_data = api_response["response"]

                # Try to parse structured data
                if isinstance(response_data, list) and len(response_data) > 0:
                    return pd.DataFrame(response_data)

                # If it's a string response, try to extract structured data
                # This is a simple example - you might need more sophisticated parsing

            # Try to create sample data based on intent for testing
            return self._create_sample_data_for_intent(intent)

        except Exception as e:
            st.error(f"Error extracting data: {str(e)}")
            return None

    def _create_sample_data_for_intent(self, intent: ChartIntent) -> Optional[pd.DataFrame]:
        """
        Create sample data based on intent (for testing)
        Remove this once real API integration is working
        """
        import pandas as pd
        from datetime import datetime, timedelta

        # Create sample sales/revenue data for trends/charts
        if any(col in (intent.suggested_columns or []) for col in ['sales', 'revenue']):
            dates = pd.date_range(start='2024-01-01', end='2024-12-01', freq='M')
            regions = ['North', 'South', 'East', 'West']
            data = {
                'order_date': dates,
                'total_revenue': [1000 + i*100 + (i*50 if i%2==0 else 0) for i in range(len(dates))],
                'region': (regions * ((len(dates) // len(regions)) + 1))[:len(dates)],
                'invoice_count': [10 + i*2 for i in range(len(dates))]
            }
            return pd.DataFrame(data)

        # Create sample invoice data for "top invoices this week"
        if 'invoice' in (intent.suggested_columns or []) or 'invoice' in str(intent).lower():
            today = datetime.now()
            week_start = today - timedelta(days=today.weekday())
            regions = ['North', 'South', 'East', 'West']
            data = {
                'invoice_id': [f'INV-{1000+i}' for i in range(10)],
                'customer_name': [f'Customer {chr(65+i)}' for i in range(10)],
                'invoice_amount': [5000 - i*300 for i in range(10)],  # Descending amounts
                'invoice_date': [week_start + timedelta(days=i % 7) for i in range(10)],
                'region': (regions * ((10 // len(regions)) + 1))[:10]
            }
            return pd.DataFrame(data)

        # Default: no data
        return None



    def _build_query_from_intent(self, user_message: str, intent: ChartIntent, user_role: str = None) -> Dict[str, Any]:
        """
        Build API query parameters based on detected intent

        Modify this based on your FastAPI query structure
        """

        query_params = {
            "message": user_message,
            "intent_type": "chart_request",
            "chart_type": intent.chart_type,
            "suggested_columns": intent.suggested_columns,
            "time_period": intent.time_period,
            "aggregation": intent.aggregation,
            "user_role": user_role,
            "limit": 1000  # Reasonable limit for charting
        }

        # Add role-based filters if available
        if user_role:
            query_params["role_filters"] = self._get_role_based_filters(user_role)

        return query_params

    def _get_role_based_filters(self, user_role: str) -> Dict[str, Any]:
        """
        Get role-based data filters

        Customize based on your role system
        """
        role_filters = {
            "CEO": {},  # CEO sees all data
            "Finance": {"department": "finance"},
            "Sales": {"department": "sales"},
            "Logistics": {"department": "logistics"}
        }

        return role_filters.get(user_role, {})

    def _generate_chart_response(self, config: ChartConfig, data: pd.DataFrame, user_message: str) -> str:
        """
        Generate conversational response about the created chart
        """

        data_count = len(data)
        chart_type_name = config.chart_type.title()

        response_parts = [
            f"I've created a {chart_type_name.lower()} chart showing **{config.title}**.",
            f"The chart displays data from {data_count} records."
        ]

        # Add insights based on chart type
        if config.chart_type == 'line' and data_count > 1:
            # Simple trend analysis
            y_values = data[config.y_column].dropna()
            if len(y_values) > 1:
                trend = "increasing" if y_values.iloc[-1] > y_values.iloc[0] else "decreasing"
                response_parts.append(f"The overall trend appears to be {trend}.")

        elif config.chart_type == 'bar':
            # Top performer
            if config.aggregation == 'sum':
                grouped = data.groupby(config.x_column)[config.y_column].sum()
                top_category = grouped.idxmax()
                top_value = grouped.max()
                response_parts.append(f"**{top_category}** has the highest value with {top_value:,.0f}.")

        elif config.chart_type == 'pie':
            # Distribution insights
            total_categories = data[config.x_column].nunique()
            response_parts.append(f"The chart shows distribution across {total_categories} categories.")

        # Add interaction tip
        response_parts.append("💡 *Tip: You can hover over the chart elements for detailed information, and use the toolbar for zooming and panning.*")

        return " ".join(response_parts)

    def _save_to_history(self, user_message: str, config: ChartConfig, data_shape: tuple):
        """Save chart creation to session history"""

        history_entry = {
            "timestamp": pd.Timestamp.now(),
            "user_message": user_message,
            "chart_type": config.chart_type,
            "chart_title": config.title,
            "data_records": data_shape[0],
            "x_column": config.x_column,
            "y_column": config.y_column
        }

        st.session_state.chart_history.append(history_entry)

        # Keep only last 10 charts
        if len(st.session_state.chart_history) > 10:
            st.session_state.chart_history.pop(0)

    def render_chart_in_chat(self, fig, response_text: str):
        """
        Render chart in Streamlit chat interface
        """

        # Display the response text
        with st.chat_message("assistant"):
            st.write(response_text)

            # Display the interactive chart
            st.plotly_chart(fig, use_container_width=True, key=f"chart_{len(st.session_state.chart_history)}")

            # Add download buttons
            col1, col2, col3 = st.columns(3)

            with col1:
                # Download chart as HTML
                html_content = self.chart_generator.chart_to_html(fig)
                st.download_button(
                    label="📊 Download Chart (HTML)",
                    data=html_content,
                    file_name="chart.html",
                    mime="text/html"
                )

            with col2:
                # Download chart data as CSV
                if hasattr(fig, 'data') and len(fig.data) > 0:
                    # Extract data from chart
                    chart_data = self._extract_chart_data(fig)
                    if chart_data is not None:
                        csv_buffer = io.StringIO()
                        chart_data.to_csv(csv_buffer, index=False)
                        st.download_button(
                            label="📥 Download Data (CSV)",
                            data=csv_buffer.getvalue(),
                            file_name="chart_data.csv",
                            mime="text/csv"
                        )

            with col3:
                # Download chart as JSON
                json_content = self.chart_generator.chart_to_json(fig)
                st.download_button(
                    label="🔧 Download Chart (JSON)",
                    data=json_content,
                    file_name="chart.json",
                    mime="application/json"
                )

    def _extract_chart_data(self, fig) -> Optional[pd.DataFrame]:
        """Extract data from Plotly figure for CSV download"""
        try:
            if fig.data:
                # Extract x and y data from first trace
                trace = fig.data[0]
                data_dict = {}

                if hasattr(trace, 'x') and trace.x is not None:
                    data_dict['x'] = trace.x
                if hasattr(trace, 'y') and trace.y is not None:
                    data_dict['y'] = trace.y

                if data_dict:
                    return pd.DataFrame(data_dict)
            return None
        except:
            return None

    def show_chart_history(self):
        """Display chart creation history in sidebar"""

        if st.session_state.chart_history:
            st.sidebar.write("### 📊 Recent Charts")

            for i, entry in enumerate(reversed(st.session_state.chart_history[-5:])):
                with st.sidebar.expander(f"Chart {len(st.session_state.chart_history)-i}: {entry['chart_type'].title()}"):
                    st.write(f"**Query**: {entry['user_message'][:50]}...")
                    st.write(f"**Title**: {entry['chart_title']}")
                    st.write(f"**Records**: {entry['data_records']}")
                    st.write(f"**Time**: {entry['timestamp'].strftime('%H:%M:%S')}")

    def _prepare_data_for_chart(self, data: pd.DataFrame, chart_type: str) -> pd.DataFrame:
        """Convert data types for proper chart rendering with universal date handling"""
        
        # Make a copy to avoid modifying original data
        plot_data = data.copy()
        
        for col in plot_data.columns:
            col_upper = col.upper()
            
            # PRIORITY 1: Handle date columns with comprehensive detection
            if any(keyword in col_upper for keyword in ['DATE', 'DATO', 'DAT', 'TIME', 'OHDAO', 'CREATED', 'UPDATED', 'MODIFIED']):
                print(f"Detected potential date column: {col}")
                plot_data[col] = self._convert_to_datetime(plot_data[col])
                
            # PRIORITY 2: Handle numeric columns (amounts, counts, etc.)
            elif any(keyword in col_upper for keyword in ['TOTAL', 'SALES', 'AMOUNT', 'VALUE', 'COUNT', 'REVENUE', 'ORDERS', 'PRICE', 'COST']):
                try:
                    # Try to convert to numeric
                    numeric_series = pd.to_numeric(plot_data[col], errors='coerce')
                    # If most values convert successfully, use the numeric version
                    if numeric_series.notna().sum() > len(plot_data) * 0.8:
                        plot_data[col] = numeric_series
                        print(f"Converted {col} to numeric")
                except:
                    pass
                    
            # PRIORITY 3: Handle other object columns that might be numeric
            elif plot_data[col].dtype == 'object':
                # Check if it looks like numbers stored as strings
                try:
                    # Sample a few values to see if they're numeric
                    sample = plot_data[col].dropna().head(5)
                    numeric_sample = pd.to_numeric(sample, errors='coerce')
                    if numeric_sample.notna().sum() == len(sample) and len(sample) > 0:
                        plot_data[col] = pd.to_numeric(plot_data[col], errors='coerce')
                        print(f"Converted string numbers in {col} to numeric")
                except:
                    pass
        
        # For pie charts, ensure we have at least one numeric column
        if chart_type == 'pie':
            numeric_cols = plot_data.select_dtypes(include=['float64', 'int64', 'float32', 'int32']).columns
            if len(numeric_cols) == 0:
                print("Warning: No numeric columns found for pie chart")
        
        print(f"Data prepared for {chart_type} chart:")
        print(f"Shape: {plot_data.shape}")
        print(f"Data types: {plot_data.dtypes.to_dict()}")
        
        return plot_data



# Integration helper function
def integrate_with_existing_chat(chat_integration: ChatChartIntegration, sql_query: str, user_role: str = None, original_message: str = ""):
    """
    Helper function to integrate with existing chat logic
    """
    
    # Always treat SQL queries as chart requests
    is_chart_request, response_text, chart_figure = chat_integration.process_user_message(
        sql_query, user_role, original_message
    )
    
    if chart_figure is not None:
        # Successfully created chart
        chat_integration.render_chart_in_chat(chart_figure, response_text)
        return True
    else:
        # Chart request but failed to create
        with st.chat_message("assistant"):
            st.write(response_text)
        return True

