from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import json

@dataclass
class ChartConfig:
    chart_type: str
    title: str
    x_column: str
    y_column: str
    color_column: Optional[str] = None
    size_column: Optional[str] = None
    aggregation: Optional[str] = None
    time_period: Optional[str] = None
    filters: Dict[str, Any] = None
    chart_params: Dict[str, Any] = None

class ChartConfigBuilder:
    def __init__(self):
        # Column type patterns
        self.date_patterns = ['date', 'time', 'created', 'updated', 'timestamp']
        self.categorical_patterns = ['name', 'type', 'category', 'region', 'status', 'country', 'state']
        self.numerical_patterns = ['amount', 'total', 'count', 'sum', 'avg', 'revenue', 'sales', 'price', 'cost']

        # Default column mappings for common business data
        self.default_columns = {
            'sales': ['revenue', 'sales', 'income', 'amount', 'total_sales', 'total_revenue'],
            'date': ['date', 'created_date', 'order_date', 'timestamp', 'time'],
            'customer': ['customer_name', 'customer_id', 'client', 'customer_type'],
            'product': ['product_name', 'product_id', 'item'],
            'region': ['region', 'location', 'country', 'state'],
            'category': ['category', 'type', 'group', 'customer_type']
        }

        # Chart type specific configurations
        self.chart_defaults = {
            'bar': {
                'orientation': 'v',
                'color_scale': 'viridis',
                'show_values': True
            },
            'line': {
                'line_shape': 'linear',
                'markers': True,
                'fill': None
            },
            'pie': {
                'hole': 0.3,  # Donut chart
                'show_labels': True,
                'show_values': True
            },
            'scatter': {
                'size_max': 20,
                'opacity': 0.7
            },
            'area': {
                'fill': 'tonexty',
                'line_shape': 'linear'
            }
        }

    def build_config(self, intent, available_columns: List[str] = None) -> ChartConfig:
        """
        Build chart configuration from detected intent and available data columns
        """
        if available_columns is None:
            available_columns = []

        # Determine chart type
        chart_type = intent.chart_type or self._suggest_chart_type(intent)

        # Map suggested columns to actual columns based on chart type
        x_col, y_col, color_col = self._map_columns_by_chart_type(intent, available_columns, chart_type)

        # Generate title
        title = self._generate_title(intent, chart_type, x_col, y_col)

        # Get chart-specific parameters
        chart_params = self.chart_defaults.get(chart_type, {}).copy()

        # Apply aggregation settings
        aggregation = intent.aggregation or self._suggest_aggregation(chart_type, y_col)

        return ChartConfig(
            chart_type=chart_type,
            title=title,
            x_column=x_col,
            y_column=y_col,
            color_column=color_col,
            aggregation=aggregation,
            time_period=intent.time_period,
            chart_params=chart_params
        )

    def _suggest_chart_type(self, intent) -> str:
        """Suggest chart type based on intent context"""
        suggested_columns = intent.suggested_columns or []

        # If date column suggested, likely a time series
        if 'date' in suggested_columns:
            return 'line'

        # If multiple categories, bar chart works well
        if len(suggested_columns) >= 2:
            return 'bar'

        # Default to bar chart
        return 'bar'

    def _map_columns_by_chart_type(self, intent, available_columns: List[str], chart_type: str) -> tuple:
        """Map columns based on chart type requirements"""

        if chart_type == 'line':
            return self._map_line_chart_columns(intent, available_columns)
        elif chart_type == 'bar':
            return self._map_bar_chart_columns(intent, available_columns)
        elif chart_type == 'pie':
            return self._map_pie_chart_columns(intent, available_columns)
        else:
            return self._map_default_columns(intent, available_columns)

    def _map_line_chart_columns(self, intent, available_columns: List[str]) -> tuple:
        """Map columns for line charts (typically time-series)"""
        suggested = intent.suggested_columns or []

        # PRIORITY 1: Look for date column in intent or by type
        x_column = None
        if 'date' in suggested:
            x_column = self._find_best_column_match(['date'], available_columns)

        if not x_column:
            x_column = self._find_column_by_type(available_columns, 'date') or \
                      available_columns[0] if available_columns else 'date'

        # PRIORITY 2: Look for numerical column in intent suggestions
        numerical_cols = [col for col in available_columns if col != x_column]
        y_column = None

        for suggestion in suggested:
            if suggestion != 'date':  # Skip date suggestions for Y axis
                match = self._find_best_column_match([suggestion], numerical_cols)
                if match:
                    y_column = match
                    break

        # Fall back to numerical detection
        if not y_column:
            y_column = self._find_column_by_type(numerical_cols, 'numerical') or \
                      (numerical_cols[0] if numerical_cols else available_columns[0]) if available_columns else 'value'

        # Color: Optional grouping column
        color_column = self._find_best_column_match(['region', 'category'], available_columns)

        return x_column, y_column, color_column

    def _map_bar_chart_columns(self, intent, available_columns: List[str]) -> tuple:
        """Map columns for bar charts"""
        # PRIORITY 1: Use intent-suggested columns first
        suggested = intent.suggested_columns or []

        # Look for categorical column in user intent first
        x_column = None
        for suggestion in suggested:
            match = self._find_best_column_match([suggestion], available_columns)
            if match and self._is_likely_categorical(match, available_columns):
                x_column = match
                break

        # PRIORITY 2: Fall back to categorical detection if no intent match
        if not x_column:
            x_column = self._find_column_by_type(available_columns, 'categorical') or \
                      available_columns[0] if available_columns else 'category'

        # Y-axis: Numerical column (different from X), prioritize intent suggestions
        numerical_cols = [col for col in available_columns if col != x_column]

        # Look for numerical column in intent suggestions first
        y_column = None
        for suggestion in suggested:
            match = self._find_best_column_match([suggestion], numerical_cols)
            if match and self._is_likely_numerical(match):
                y_column = match
                break

        # Fall back to numerical detection
        if not y_column:
            y_column = self._find_column_by_type(numerical_cols, 'numerical') or \
                      (numerical_cols[0] if numerical_cols else available_columns[-1]) if available_columns else 'value'

        # Color: Use X column for coloring by default
        color_column = x_column if x_column and x_column in available_columns else None

        return x_column, y_column, color_column

    def _map_pie_chart_columns(self, intent, available_columns: List[str]) -> tuple:
        """Map columns for pie charts"""
        # X-axis: Categorical column (labels)
        x_column = self._find_column_by_type(available_columns, 'categorical') or \
                  self._find_best_column_match(['category', 'customer', 'region'], available_columns) or \
                  available_columns[0] if available_columns else 'category'

        # Y-axis: Numerical column for values (or use count aggregation)
        numerical_cols = [col for col in available_columns if col != x_column]
        y_column = self._find_column_by_type(numerical_cols, 'numerical') or x_column

        return x_column, y_column, None

    def _map_default_columns(self, intent, available_columns: List[str]) -> tuple:
        """Default column mapping"""
        x_column = available_columns[0] if available_columns else 'category'
        y_column = available_columns[1] if len(available_columns) > 1 else available_columns[0] if available_columns else 'value'
        color_column = None

        return x_column, y_column, color_column

    def _is_likely_categorical(self, column: str, available_columns: List[str]) -> bool:
        """Check if column is likely categorical"""
        col_lower = column.lower()

        # Check against categorical patterns
        if any(pattern in col_lower for pattern in self.categorical_patterns):
            return True

        # If it's not clearly numerical, assume it's categorical
        if not any(pattern in col_lower for pattern in self.numerical_patterns):
            return True

        return False

    def _is_likely_numerical(self, column: str) -> bool:
        """Check if column is likely numerical"""
        col_lower = column.lower()
        return any(pattern in col_lower for pattern in self.numerical_patterns)

    def _find_column_by_type(self, available_columns: List[str], column_type: str) -> Optional[str]:
        """Find column by type (date, categorical, numerical)"""
        if not available_columns:
            return None

        if column_type == 'date':
            for col in available_columns:
                if any(pattern in col.lower() for pattern in self.date_patterns):
                    return col

        elif column_type == 'categorical':
            for col in available_columns:
                if any(pattern in col.lower() for pattern in self.categorical_patterns):
                    return col

        elif column_type == 'numerical':
            for col in available_columns:
                if any(pattern in col.lower() for pattern in self.numerical_patterns):
                    return col

        return None

    def _find_best_column_match(self, intent_columns: List[str], available_columns: List[str]) -> Optional[str]:
        """Find best matching column from available columns"""
        if not available_columns:
            return None

        # Direct matches first
        for intent_col in intent_columns:
            for avail_col in available_columns:
                if intent_col.lower() in avail_col.lower():
                    return avail_col

        # Semantic matches using default mappings
        for intent_col in intent_columns:
            if intent_col in self.default_columns:
                for synonym in self.default_columns[intent_col]:
                    for avail_col in available_columns:
                        if synonym.lower() in avail_col.lower():
                            return avail_col

        return None

    def _suggest_aggregation(self, chart_type: str, y_column: str) -> Optional[str]:
        """Suggest appropriate aggregation based on chart type and column"""
        if not y_column:
            return None

        y_lower = y_column.lower()

        # Revenue/sales typically summed
        if any(word in y_lower for word in ['revenue', 'sales', 'amount', 'total']):
            return 'sum'

        # Counts
        if any(word in y_lower for word in ['count', 'number', 'qty', 'quantity']):
            return 'sum'

        # For pie charts, usually count
        if chart_type == 'pie':
            return 'count'

        return None

    def _generate_title(self, intent, chart_type: str, x_col: str, y_col: str) -> str:
        """Generate descriptive chart title"""

        # Clean column names for display
        x_display = x_col.replace('_', ' ').title() if x_col else 'Categories'
        y_display = y_col.replace('_', ' ').title() if y_col else 'Values'

        if chart_type == 'line':
            if intent.time_period:
                return f"{y_display} Trend ({intent.time_period.title()})"
            else:
                return f"{y_display} Over Time"

        elif chart_type == 'bar':
            return f"{y_display} by {x_display}"

        elif chart_type == 'pie':
            return f"{x_display} Distribution"

        elif chart_type == 'scatter':
            return f"{y_display} vs {x_display}"

        else:
            return f"{y_display} Analysis"