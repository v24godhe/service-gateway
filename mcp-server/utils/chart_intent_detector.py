import re
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

@dataclass
class ChartIntent:
    is_chart_request: bool
    confidence: float
    chart_type: Optional[str] = None
    suggested_columns: List[str] = None
    time_period: Optional[str] = None
    aggregation: Optional[str] = None

class ChartIntentDetector:
    def __init__(self):
        # Chart type keywords
        self.chart_keywords = {
            'bar': ['bar chart', 'bar graph', 'bars', 'column chart', 'histogram'],
            'line': ['line chart', 'line graph', 'trend', 'time series', 'over time'],
            'pie': ['pie chart', 'pie graph', 'distribution', 'percentage', 'proportion'],
            'scatter': ['scatter plot', 'scatter chart', 'correlation', 'relationship'],
            'area': ['area chart', 'filled chart', 'area graph']
        }

        # General visualization keywords
        self.viz_keywords = [
            'plot', 'chart', 'graph', 'visualize', 'show me', 'display',
            'compare', 'analysis', 'trend', 'pattern', 'distribution'
        ]

        # Data aggregation keywords
        self.aggregation_keywords = {
            'sum': ['total', 'sum', 'add up', 'aggregate'],
            'count': ['count', 'number of', 'how many'],
            'average': ['average', 'mean', 'avg'],
            'max': ['maximum', 'max', 'highest', 'peak'],
            'min': ['minimum', 'min', 'lowest', 'bottom']
        }

        # Time period keywords
        self.time_keywords = {
            'daily': ['daily', 'per day', 'each day'],
            'weekly': ['weekly', 'per week', 'each week'],
            'monthly': ['monthly', 'per month', 'each month'],
            'yearly': ['yearly', 'per year', 'annually']
        }

    def detect_chart_intent(self, user_message: str) -> ChartIntent:
        """
        Detect if user message requires a chart and extract parameters
        """
        message_lower = user_message.lower()

        # Check for visualization keywords
        viz_score = self._calculate_viz_score(message_lower)

        if viz_score < 0.3:
            return ChartIntent(is_chart_request=False, confidence=0.0)

        # Detect chart type
        chart_type = self._detect_chart_type(message_lower)

        # Extract suggested columns (basic keyword extraction)
        suggested_columns = self._extract_column_hints(message_lower)

        # Detect time period
        time_period = self._detect_time_period(message_lower)

        # Detect aggregation
        aggregation = self._detect_aggregation(message_lower)

        return ChartIntent(
            is_chart_request=True,
            confidence=viz_score,
            chart_type=chart_type,
            suggested_columns=suggested_columns,
            time_period=time_period,
            aggregation=aggregation
        )

    def _calculate_viz_score(self, message: str) -> float:
        """Calculate confidence score for visualization intent"""
        score = 0.0

        # Direct visualization keywords
        for keyword in self.viz_keywords:
            if keyword in message:
                score += 0.4

        # Chart type keywords
        for chart_type, keywords in self.chart_keywords.items():
            for keyword in keywords:
                if keyword in message:
                    score += 0.5
                    break

        # Question patterns that suggest visualization
        question_patterns = [
            r'show me.*trend',
            r'how.*compare',
            r'what.*looks? like',
            r'visualize.*data'
        ]

        for pattern in question_patterns:
            if re.search(pattern, message):
                score += 0.3

        return min(score, 1.0)

    def _detect_chart_type(self, message: str) -> Optional[str]:
        """Detect specific chart type from message"""
        for chart_type, keywords in self.chart_keywords.items():
            for keyword in keywords:
                if keyword in message:
                    return chart_type

        # Default suggestions based on context
        if any(word in message for word in ['over time', 'trend', 'monthly', 'daily']):
            return 'line'
        elif any(word in message for word in ['compare', 'vs', 'versus']):
            return 'bar'
        elif any(word in message for word in ['distribution', 'percentage']):
            return 'pie'

        return None

    def _extract_column_hints(self, message: str) -> List[str]:
        """Extract potential column names from message"""
        # Common business column hints
        column_hints = {
            'sales': ['sales', 'revenue', 'income'],
            'date': ['date', 'time', 'when', 'monthly', 'daily'],
            'customer': ['customer', 'client', 'user'],
            'product': ['product', 'item', 'service'],
            'amount': ['amount', 'quantity', 'total'],
            'region': ['region', 'location', 'area', 'country']
        }

        found_columns = []
        for column, keywords in column_hints.items():
            if any(keyword in message for keyword in keywords):
                found_columns.append(column)

        return found_columns

    def _detect_time_period(self, message: str) -> Optional[str]:
        """Detect time period from message"""
        for period, keywords in self.time_keywords.items():
            if any(keyword in message for keyword in keywords):
                return period
        return None

    def _detect_aggregation(self, message: str) -> Optional[str]:
        """Detect aggregation type from message"""
        for agg_type, keywords in self.aggregation_keywords.items():
            if any(keyword in message for keyword in keywords):
                return agg_type
        return None

# Test function
def test_chart_intent_detector():
    detector = ChartIntentDetector()

    test_messages = [
        "Show me sales trend over time",
        "Create a bar chart of revenue by region",
        "What's the weather like today?",
        "Plot customer distribution by age",
        "Compare monthly sales figures"
    ]

    for message in test_messages:
        intent = detector.detect_chart_intent(message)
        print(f"Message: {message}")
        print(f"Chart Intent: {intent}")
        print("-" * 50)

if __name__ == "__main__":
    test_chart_intent_detector()