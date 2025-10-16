# utils/theme.py

"""
Theme definitions for Analytics Chat Assistant and other Streamlit pages.
Each theme is a complete CSS <style> block that can be injected via st.markdown.
"""

THEMES = {
    "Light": """
    <style>
        :root {
            --primary-color: #0073AE;
            --secondary-bg: #0F2436;
            --background-color: #F4F6FA;
            --text-color: #0F2436;
            --radius: 12px;
        }

        .main {
            background-color: var(--background-color);
        }

        h1, h2, h3 {
            color: var(--text-color);
            font-weight: 700;
        }

        /* Chat Bubbles */
        .user-message {
            background: var(--primary-color);
            color: white;
            padding: 1rem;
            border-radius: var(--radius);
            margin: 0.5rem 0;
            max-width: 75%;
            margin-left: auto;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            transition: all 0.2s ease;
        }

        .assistant-message {
            background: white;
            padding: 1rem;
            border-radius: var(--radius);
            margin: 0.5rem 0;
            max-width: 75%;
            border-left: 5px solid var(--primary-color);
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            transition: all 0.2s ease;
        }

        /* Buttons */
        .stButton>button {
            background-color: var(--primary-color);
            color: white;
            border: none;
            border-radius: 8px;
            font-weight: 600;
            padding: 0.5rem 1.5rem;
            transition: all 0.2s ease;
        }
        .stButton>button:hover {
            background-color: #005a8a;
            transform: translateY(-2px);
        }

        /* Inputs */
        .stTextInput>div>div>input {
            border-radius: 30px;
            border: 2px solid var(--primary-color);
            padding: 0.8rem 1.2rem;
        }

        /* Sidebar styling */
        [data-testid="stSidebar"] {
            background-color: var(--secondary-bg);
            color: white;
        }

        [data-testid="stSidebar"] h1,
        [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3 {
            color: white;
        }

        .metric-label, .metric-value {
            color: white !important;
        }

        [data-testid="stSidebar"] * {
            color: white !important;
        }

        [data-testid="stSidebar"] .stSelectbox label,
        [data-testid="stSidebar"] .stButton button,
        [data-testid="stSidebar"] .stMarkdown,
        [data-testid="stSidebar"] p,
        [data-testid="stSidebar"] span {
            color: white !important;
        }

        [data-testid="stSidebar"] .stSelectbox > div > div {
            background-color: rgba(255,255,255,0.1);
            color: white;
        }
    </style>
    """,

    "Compact": """
    <style>
        :root {
            --primary-color: #0073AE;
            --background-color: #f2f4f8;
            --secondary-bg: #0F2436;
            --text-color: #0F2436;
        }

        .main {
            background-color: var(--background-color);
        }

        .stButton>button {
            background-color: var(--primary-color);
            color: white;
            border-radius: 8px;
            padding: 0.5rem 2rem;
            font-weight: 600;
            border: none;
            transition: all 0.3s;
        }

        .stButton>button:hover {
            background-color: #005a8a;
            transform: translateY(-2px);
        }

        .approve-button>button {
            background-color: #28a745;
        }

        .deny-button>button {
            background-color: #dc3545;
        }

        h1, h2, h3 {
            color: var(--text-color);
            font-weight: 700;
        }

        .metric-card {
            background: white;
            padding: 20px;
            border-radius: 10px;
            border-left: 4px solid var(--primary-color);
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }

        .request-card {
            background: white;
            padding: 20px;
            border-radius: 10px;
            margin: 10px 0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            border-left: 4px solid #ffc107;
        }

        .approved-card {
            border-left: 4px solid #28a745;
        }

        .denied-card {
            border-left: 4px solid #dc3545;
        }

        .stTextInput>div>div>input {
            border-radius: 8px;
            border: 2px solid var(--primary-color);
        }
    </style>
    """
}