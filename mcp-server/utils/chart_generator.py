import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from typing import Dict, List, Any, Optional
import json

from .chart_config import ChartConfig

class PlotlyChartGenerator:
    def __init__(self):
        # Color palettes for different chart types
        self.color_palettes = {
            'default': px.colors.qualitative.Set3,
            'professional': ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b'],
            'corporate': ['#003f5c', '#58508d', '#bc5090', '#ff6361', '#ffa600'],
            'cool': px.colors.sequential.Blues,
            'warm': px.colors.sequential.Reds
        }

    def generate_chart(self, config: ChartConfig, data: pd.DataFrame,
                      color_palette: str = 'professional') -> go.Figure:
        """
        Generate interactive Plotly chart based on configuration and data
        """
        if data.empty:
            return self._create_empty_chart("No data available")

        # Validate columns exist in data
        missing_cols = self._validate_columns(config, data)
        if missing_cols:
            return self._create_error_chart(f"Missing columns: {missing_cols}")

        # Apply aggregation if needed
        if config.aggregation:
            data = self._apply_aggregation(data, config)

        # Generate chart based on type
        try:
            if config.chart_type == 'bar':
                fig = self._create_bar_chart(config, data, color_palette)
            elif config.chart_type == 'line':
                fig = self._create_line_chart(config, data, color_palette)
            elif config.chart_type == 'pie':
                fig = self._create_pie_chart(config, data, color_palette)
            elif config.chart_type == 'scatter':
                fig = self._create_scatter_chart(config, data, color_palette)
            elif config.chart_type == 'area':
                fig = self._create_area_chart(config, data, color_palette)
            else:
                fig = self._create_bar_chart(config, data, color_palette)  # Default fallback

            # Apply common formatting
            self._apply_common_formatting(fig, config)

            return fig

        except Exception as e:
            return self._create_error_chart(f"Error generating chart: {str(e)}")

    def _validate_columns(self, config: ChartConfig, data: pd.DataFrame) -> List[str]:
        """Validate that required columns exist in data"""
        missing = []
        required_cols = [config.x_column, config.y_column]

        if config.color_column:
            required_cols.append(config.color_column)

        for col in required_cols:
            if col and col not in data.columns:
                missing.append(col)

        return missing

    def _apply_aggregation(self, data: pd.DataFrame, config: ChartConfig) -> pd.DataFrame:
        """Apply aggregation to data based on configuration"""
        if not config.aggregation or not config.x_column or not config.y_column:
            return data

        group_cols = [config.x_column]
        if config.color_column and config.color_column != config.x_column:
            group_cols.append(config.color_column)

        try:
            if config.aggregation == 'sum':
                return data.groupby(group_cols)[config.y_column].sum().reset_index()
            elif config.aggregation == 'count':
                return data.groupby(group_cols).size().reset_index(name=config.y_column)
            elif config.aggregation == 'average' or config.aggregation == 'mean':
                return data.groupby(group_cols)[config.y_column].mean().reset_index()
            elif config.aggregation == 'max':
                return data.groupby(group_cols)[config.y_column].max().reset_index()
            elif config.aggregation == 'min':
                return data.groupby(group_cols)[config.y_column].min().reset_index()
            else:
                return data
        except Exception:
            return data

    def _create_bar_chart(self, config: ChartConfig, data: pd.DataFrame, color_palette: str) -> go.Figure:
        """Create bar chart"""
        colors = self.color_palettes.get(color_palette, self.color_palettes['professional'])

        if config.color_column and config.color_column in data.columns:
            fig = px.bar(
                data,
                x=config.x_column,
                y=config.y_column,
                color=config.color_column,
                title=config.title,
                color_discrete_sequence=colors
            )
        else:
            fig = px.bar(
                data,
                x=config.x_column,
                y=config.y_column,
                title=config.title,
                color_discrete_sequence=colors
            )

        # Add value labels on bars if configured
        if config.chart_params and config.chart_params.get('show_values', False):
            fig.update_traces(texttemplate='%{y}', textposition='outside')

        return fig

    def _create_line_chart(self, config: ChartConfig, data: pd.DataFrame, color_palette: str) -> go.Figure:
        """Create line chart"""
        colors = self.color_palettes.get(color_palette, self.color_palettes['professional'])

        if config.color_column and config.color_column in data.columns:
            fig = px.line(
                data,
                x=config.x_column,
                y=config.y_column,
                color=config.color_column,
                title=config.title,
                color_discrete_sequence=colors
            )
        else:
            fig = px.line(
                data,
                x=config.x_column,
                y=config.y_column,
                title=config.title,
                color_discrete_sequence=colors
            )

        # Add markers if configured
        if config.chart_params and config.chart_params.get('markers', False):
            fig.update_traces(mode='lines+markers')

        return fig

    def _create_pie_chart(self, config: ChartConfig, data: pd.DataFrame, color_palette: str) -> go.Figure:
        """Create pie chart"""
        colors = self.color_palettes.get(color_palette, self.color_palettes['professional'])

        fig = px.pie(
            data,
            names=config.x_column,
            values=config.y_column,
            title=config.title,
            color_discrete_sequence=colors
        )

        # Apply hole for donut chart if configured
        if config.chart_params and 'hole' in config.chart_params:
            fig.update_traces(hole=config.chart_params['hole'])

        return fig

    def _create_scatter_chart(self, config: ChartConfig, data: pd.DataFrame, color_palette: str) -> go.Figure:
        """Create scatter plot"""
        colors = self.color_palettes.get(color_palette, self.color_palettes['professional'])

        if config.color_column and config.color_column in data.columns:
            fig = px.scatter(
                data,
                x=config.x_column,
                y=config.y_column,
                color=config.color_column,
                title=config.title,
                color_discrete_sequence=colors
            )
        else:
            fig = px.scatter(
                data,
                x=config.x_column,
                y=config.y_column,
                title=config.title,
                color_discrete_sequence=colors
            )

        return fig

    def _create_area_chart(self, config: ChartConfig, data: pd.DataFrame, color_palette: str) -> go.Figure:
        """Create area chart"""
        colors = self.color_palettes.get(color_palette, self.color_palettes['professional'])

        if config.color_column and config.color_column in data.columns:
            fig = px.area(
                data,
                x=config.x_column,
                y=config.y_column,
                color=config.color_column,
                title=config.title,
                color_discrete_sequence=colors
            )
        else:
            fig = px.area(
                data,
                x=config.x_column,
                y=config.y_column,
                title=config.title,
                color_discrete_sequence=colors
            )

        return fig

    def _apply_common_formatting(self, fig: go.Figure, config: ChartConfig):
        """Apply common formatting to all charts"""
        # Update layout for better appearance
        fig.update_layout(
            font=dict(size=12),
            title_font_size=16,
            showlegend=True,
            height=500,
            margin=dict(l=50, r=50, t=80, b=50)
        )

        # Update axes labels
        if config.x_column:
            x_label = config.x_column.replace('_', ' ').title()
            fig.update_xaxes(title_text=x_label)

        if config.y_column:
            y_label = config.y_column.replace('_', ' ').title()
            fig.update_yaxes(title_text=y_label)

        # Enable hover interactions
        fig.update_traces(hovertemplate='<extra></extra>')

    def _create_empty_chart(self, message: str) -> go.Figure:
        """Create empty chart with message"""
        fig = go.Figure()
        fig.add_annotation(
            text=message,
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16)
        )
        fig.update_layout(
            title="No Data Available",
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            height=400
        )
        return fig

    def _create_error_chart(self, error_message: str) -> go.Figure:
        """Create error chart with message"""
        fig = go.Figure()
        fig.add_annotation(
            text=f"Error: {error_message}",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=14, color="red")
        )
        fig.update_layout(
            title="Chart Generation Error",
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            height=400
        )
        return fig

    def chart_to_html(self, fig: go.Figure) -> str:
        """Convert chart to HTML string for embedding"""
        return fig.to_html(include_plotlyjs='cdn', div_id="chart-div")

    def chart_to_json(self, fig: go.Figure) -> str:
        """Convert chart to JSON for API responses"""
        return fig.to_json()

# Test function
def test_chart_generator():
    import pandas as pd
    from chart_intent_detector import ChartIntentDetector
    from chart_config import ChartConfigBuilder

    # Create sample data
    sample_data = pd.DataFrame({
        'order_date': pd.date_range('2024-01-01', periods=12, freq='M'),
        'total_revenue': [1000, 1200, 1100, 1300, 1500, 1400, 1600, 1700, 1800, 1900, 2000, 2100],
        'region': ['North', 'South', 'East', 'West', 'North', 'South', 'East', 'West', 'North', 'South', 'East', 'West']
    })

    detector = ChartIntentDetector()
    config_builder = ChartConfigBuilder()
    chart_generator = PlotlyChartGenerator()

    # Test line chart
    intent = detector.detect_chart_intent("Show me sales trend over time")
    config = config_builder.build_config(intent, list(sample_data.columns))
    fig = chart_generator.generate_chart(config, sample_data)

    print(f"Generated {config.chart_type} chart: {config.title}")
    print(f"Chart JSON length: {len(chart_generator.chart_to_json(fig))} characters")

    # Save as HTML for testing
    html_content = chart_generator.chart_to_html(fig)
    with open('test_chart.html', 'w') as f:
        f.write(html_content)
    print("Test chart saved as 'test_chart.html'")

if __name__ == "__main__":
    test_chart_generator()