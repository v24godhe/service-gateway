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

    def process_user_message(self, user_message: str, user_role: str = None) -> Tuple[bool, str, Optional[Any]]:
        """
        Process user message and determine if chart generation is needed

        Returns:
            (is_chart_request, response_text, chart_figure)
        """
        # Step 1: Detect chart intent
        intent = self.intent_detector.detect_chart_intent(user_message)

        if not intent.is_chart_request:
            return False, None, None

        # Step 2: Get data from FastAPI
        try:
            data, columns = self._fetch_data_from_api(user_message, intent, user_role)

            if data is None or data.empty:
                return True, "I couldn't find any data to create a chart. Please try a different query or check if the data exists.", None

            # Step 3: Build chart configuration
            config = self.config_builder.build_config(intent, columns)

            # Step 4: Generate chart
            fig = self.chart_generator.generate_chart(config, data)

            # Step 5: Generate response text
            response_text = self._generate_chart_response(config, data, user_message)

            # Step 6: Save to history
            self._save_to_history(user_message, config, data.shape)

            return True, response_text, fig

        except Exception as e:
            error_msg = f"Sorry, I encountered an error while creating the chart: {str(e)}"
            return True, error_msg, None

    def _fetch_data_from_api(self, user_message: str, intent: ChartIntent, user_role: str = None) -> Tuple[Optional[pd.DataFrame], list]:
        try:
            # Choose some valid user for demo; update with your actual user system
            username = "ceo" if not user_role else user_role.lower()
            headers = {
                "Content-Type": "application/json",
                "X-Username": username
            }
            payload = {
                "query": user_message
            }
            response = requests.post(
                f"{self.fastapi_base_url}/api/execute-query",
                headers=headers,
                json=payload,
                timeout=30
            )
            if response.status_code == 200:
                result = response.json()
                # The typical structure is result['data']['rows'], fix if needed!
                records = result.get("data", {}).get("rows", [])
                if records:
                    df = pd.DataFrame(records)
                    return df, list(df.columns)
            else:
                st.error(f"API Error: {response.status_code} - {response.text}")
            return None, []
        except Exception as e:
            st.error(f"Data fetch error: {str(e)}")
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

# Integration helper function
def integrate_with_existing_chat(chat_integration: ChatChartIntegration, user_message: str, user_role: str = None):
    """
    Helper function to integrate with existing chat logic

    Call this in your existing chat message processing
    """

    is_chart_request, response_text, chart_figure = chat_integration.process_user_message(user_message, user_role)

    if is_chart_request:
        if chart_figure is not None:
            # Successfully created chart
            chat_integration.render_chart_in_chat(chart_figure, response_text)
            return True  # Indicate chart was processed
        else:
            # Chart request but failed to create
            with st.chat_message("assistant"):
                st.write(response_text)
            return True

    return False  # Not a chart request, continue with normal chat logic