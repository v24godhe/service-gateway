import streamlit as st
import httpx
import os
from dotenv import load_dotenv
from openai import OpenAI
from utils.system_admin import SystemAdmin
from utils.theme import THEMES
from utils.metadata_manager import generate_friendly_names

load_dotenv()

# Auth check
system_admin = SystemAdmin()
if "username" not in st.session_state or not st.session_state.username:
    st.error("🔐 Please login first")
    st.stop()

if not system_admin.is_admin(st.session_state.username):
    st.error("⛔ Access Denied: Dev Admin privileges required.")
    st.stop()

st.set_page_config(
    page_title="Query Builder Config",
    page_icon="🔧",
    layout="wide"
)

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://10.200.0.2:8080")

# Initialize session state
if "configured_tables" not in st.session_state:
    st.session_state.configured_tables = []
if "search_results" not in st.session_state:
    st.session_state.search_results = []
if "selected_table" not in st.session_state:
    st.session_state.selected_table = None
if "editing_table" not in st.session_state:
    st.session_state.editing_table = None
if "column_config" not in st.session_state:
    st.session_state.column_config = {}



def load_configured_tables(system_id):
    """Load only configured tables - fast!"""
    try:
        response = httpx.get(
            f"{GATEWAY_URL}/api/{system_id}/schema-configured",
            timeout=30.0
        )

        if response.status_code == 200:
            data = response.json()
            st.session_state.configured_tables = data.get('configured_tables', [])
            return True
        else:
            st.error(f"Failed: {response.text}")
            return False

    except Exception as e:
        st.error(f"Error: {e}")
        return False

def search_unconfigured_tables(system_id, search_term):
    """Search for tables by name"""
    try:
        response = httpx.get(
            f"{GATEWAY_URL}/api/{system_id}/schema-search",
            params={'search_term': search_term, 'limit': 20},
            timeout=30.0
        )

        if response.status_code == 200:
            data = response.json()
            st.session_state.search_results = data.get('results', [])
            return True
        else:
            st.error(f"Search failed: {response.text}")
            return False

    except Exception as e:
        st.error(f"Error: {e}")
        return False

def load_table_columns(system_id, table_name):
    """Load columns for a specific table"""
    try:
        response = httpx.get(
            f"{GATEWAY_URL}/api/{system_id}/schema",
            timeout=30.0
        )

        if response.status_code == 200:
            all_tables = response.json().get('tables', {})
            return all_tables.get(table_name, [])
        return []

    except Exception as e:
        st.error(f"Error loading columns: {e}")
        return []

def load_table_metadata(system_id, table_name):
    """Load existing metadata for editing"""
    try:
        response = httpx.get(
            f"{GATEWAY_URL}/api/metadata/{system_id}/{table_name}",
            timeout=30.0
        )

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return None
        else:
            st.error(f"Failed to load: {response.text}")
            return None

    except Exception as e:
        st.error(f"Error: {e}")
        return None

def invalidate_cache(system_id, user_role=None):
    """Invalidate schema cache"""
    try:
        response = httpx.post(
            f"{GATEWAY_URL}/api/metadata/invalidate-cache",
            json={'system_id': system_id, 'user_role': user_role},
            timeout=10.0
        )

        if response.status_code == 200:
            st.success("✅ Cache invalidated")

    except Exception as e:
        st.warning(f"⚠️ Cache error: {e}")

# Sidebar
with st.sidebar:
    st.markdown("### 🛠️ DEV ADMIN")
    if st.button("🔧 Query Builder", use_container_width=True, disabled=True):
        pass

    st.markdown("---")
    st.success(f"👤 {st.session_state.username}")

    theme_choice = st.selectbox("🎨 Theme", list(THEMES.keys()))
    st.markdown(THEMES[theme_choice], unsafe_allow_html=True)

    if st.button("Logout", use_container_width=True):
        st.session_state.username = None
        st.rerun()

# Main content
st.title("🔧 Query Builder Configuration")
st.markdown("Configure table metadata for AI-powered queries")
st.markdown("---")

# System Selection
system_id = st.selectbox("System", ["STYR"], disabled=True)

# Control buttons
col1, col2 = st.columns([3, 1])
with col1:
    if st.button("🔄 Refresh Configured Tables", type="primary"):
        with st.spinner("Loading..."):
            load_configured_tables(system_id)
            st.success(f"✅ Loaded {len(st.session_state.configured_tables)} configured tables")

with col2:
    if st.button("🗑️ Clear Cache"):
        invalidate_cache(system_id)

st.markdown("---")

# ========== CONFIGURED TABLES ==========
st.markdown("## 📋 Configured Tables")

if st.session_state.configured_tables:
    for table in st.session_state.configured_tables:
        with st.expander(f"✅ {table['table_name']} ({table.get('table_friendly_name', 'No name')})", expanded=False):
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Total Columns", table['column_count'])
            with col2:
                st.metric("Visible", table['visible_columns'])
            with col3:
                st.metric("PII", table['pii_columns'])

            st.markdown(f"**Description:** {table.get('table_description', 'N/A')}")
            st.markdown(f"**GDPR:** {table.get('gdpr_category', 'N/A')}")
            st.markdown(f"**Sensitivity:** {table.get('data_sensitivity', 'INTERNAL')}")
            st.markdown(f"**Contains PII:** {'Yes' if table['contains_pii'] else 'No'}")

            if st.button(f"✏️ Edit", key=f"edit_{table['table_name']}"):
                st.session_state.editing_table = table['table_name']
                st.rerun()
else:
    st.info("No configured tables. Search and configure tables below.")

st.markdown("---")

# ========== SEARCH UNCONFIGURED TABLES ==========
st.markdown("## 🔍 Search Unconfigured Tables")
st.markdown("*Search by table name (e.g., KHKND, ORDER, ARTICLE)*")

col1, col2 = st.columns([4, 1])
with col1:
    search_term = st.text_input(
        "Search Table Name",
        placeholder="Type table name...",
        label_visibility="collapsed"
    )

with col2:
    search_button = st.button("🔍 Search", type="primary", use_container_width=True)

if search_button and search_term:
    with st.spinner(f"Searching for '{search_term}'..."):
        if search_unconfigured_tables(system_id, search_term):
            st.success(f"Found {len(st.session_state.search_results)} tables")

# Display search results
if st.session_state.search_results:
    st.markdown("### Search Results")
    for result in st.session_state.search_results:
        with st.expander(f"⏳ {result['table_name']} ({result['column_count']} columns)"):
            st.markdown(f"**Schema:** {result['schema']}")
            st.markdown(f"**Columns:** {result['column_count']}")

            if st.button(f"⚙️ Configure", key=f"config_{result['table_name']}"):
                st.session_state.selected_table = result['table_name']
                st.session_state.editing_table = None
                st.rerun()

elif search_term and search_button:
    st.info("No matching tables found. Try a different search term.")

st.markdown("---")

# ========== CONFIGURATION FORM ==========
if st.session_state.selected_table or st.session_state.editing_table:
    table_to_config = st.session_state.editing_table or st.session_state.selected_table
    st.markdown(f"## 🔧 Configure: {table_to_config}")

    # Load data
    existing_metadata = None
    columns = []

    if st.session_state.editing_table:
        existing_metadata = load_table_metadata(system_id, table_to_config)
        if existing_metadata:
            columns = existing_metadata['columns']
            table_info = existing_metadata['table_info']
        else:
            st.error("Failed to load metadata")
            st.stop()
    else:
        # Load columns from source DB
        with st.spinner("Loading columns..."):
            columns = load_table_columns(system_id, table_to_config)
            if not columns:
                st.error("Failed to load columns")
                st.stop()
        table_info = {}

    # Table metadata form
    st.markdown("### Table Information")
    col1, col2 = st.columns(2)

    with col1:
        table_friendly_name = st.text_input(
            "Friendly Name *",
            value=table_info.get('table_friendly_name', ''),
            placeholder="e.g., Customers, Orders"
        )

        contains_pii = st.checkbox(
            "Contains PII",
            value=table_info.get('contains_pii', False)
        )

        gdpr_category = st.selectbox(
            "GDPR Category",
            ['None', 'Personal', 'Financial', 'Health', 'Operational'],
            index=['None', 'Personal', 'Financial', 'Health', 'Operational'].index(
                table_info.get('gdpr_category', 'None')
            ) if table_info.get('gdpr_category') in ['None', 'Personal', 'Financial', 'Health', 'Operational'] else 0
        )

    with col2:
        table_description = st.text_area(
            "Description",
            value=table_info.get('table_description', ''),
            placeholder="What this table contains..."
        )

        data_sensitivity = st.selectbox(
            "Data Sensitivity",
            ['PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'SENSITIVE'],
            index=['PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'SENSITIVE'].index(
                table_info.get('data_sensitivity', 'INTERNAL')
            ) if table_info.get('data_sensitivity') in ['PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'SENSITIVE'] else 1
        )

    st.markdown("---")

    # ========== COLUMN CONFIGURATION (FIXED & SIMPLIFIED) ==========
    st.markdown("### Column Configuration")

    # AI Generate Button - SIMPLIFIED & FIXED
    if st.button("🤖 Generate Friendly Names", type="primary"):
        with st.spinner("AI generating friendly names..."):
            try:
                # Get column names
                column_names = []
                for col in columns:
                    if isinstance(col, dict):
                        column_names.append(col["column_name"])
                    else:
                        column_names.append(col.column_name)

                if column_names:
                    # Generate friendly names using AI
                    friendly_names = generate_friendly_names(column_names)
                    print(friendly_names)
                    # Apply to all columns immediately
                    for col in columns:


                        col_name = col["column_name"]
                        st.session_state.column_config[col_name] = {
                            "friendly_name": friendly_names.get(col_name, col_name),
                            "is_visible": True,
                            "data_type": col["data_type"],
                            "contains_pii": col.get("contains_pii", False) if isinstance(col, dict) else False,
                            "data_classification": col.get("data_classification", "INTERNAL") if isinstance(col, dict) else "INTERNAL"
                        }
                        
                        st.success(f"Generated {len(friendly_names)} friendly names!")
                        st.rerun()

                        # col_name = col["column_name"] if isinstance(col, dict) else col.column_name

                        # # Initialize if not exists
                        # if col_name not in st.session_state.column_config:
                        #     st.session_state.column_config[col_name] = {
                        #         "friendly_name": col_name,
                        #         "is_visible": col.get("is_visible", True) if isinstance(col, dict) else True,
                        #         "contains_pii": col.get("contains_pii", False) if isinstance(col, dict) else False,
                        #         "data_classification": col.get("data_classification", "INTERNAL") if isinstance(col, dict) else "INTERNAL"
                        #     }

                        # # Apply friendly name
                        # st.session_state.column_config[col_name]["friendly_name"] = friendly_names.get(col_name, col_name)

                    st.success(f"✅ Generated friendly names for {len(friendly_names)} columns!")
                    st.rerun()  # Refresh to show new names
                else:
                    st.warning("No columns found")
            except Exception as e:
                st.error(f"Error: {e}")

    # Display columns with configuration - SIMPLIFIED & FIXED
    st.markdown("#### Configure Each Column")

    # Header row
    col_header1, col_header2, col_header3, col_header4 = st.columns([3, 3, 1, 1])
    with col_header1:
        st.write("**Original Name (Type)**")
    with col_header2:
        st.write("**Friendly Name**")
    with col_header3:
        st.write("**Visible**")
    with col_header4:
        st.write("**PII**")

    st.divider()

    # Column rows
    for idx, col in enumerate(columns):
        col_name = col["column_name"] if isinstance(col, dict) else col.column_name
        data_type = col.get("data_type", "VARCHAR") if isinstance(col, dict) else getattr(col, "data_type", "VARCHAR")

        # Ensure config exists
        if col_name not in st.session_state.column_config:
            st.session_state.column_config[col_name] = {
                "friendly_name": col.get("friendly_name", col_name) if isinstance(col, dict) else col_name,
                "is_visible": col.get("is_visible", True) if isinstance(col, dict) else True,
                "contains_pii": col.get("contains_pii", False) if isinstance(col, dict) else False,
                "data_classification": col.get("data_classification", "INTERNAL") if isinstance(col, dict) else "INTERNAL"
            }

        # Create row
        col1, col2, col3, col4 = st.columns([3, 3, 1, 1])

        with col1:
            st.text(f"{col_name} ({data_type})")

        with col2:
            new_friendly = st.text_input(
                "friendly_name",
                value=st.session_state.column_config[col_name]["friendly_name"],
                key=f"friendly_{idx}_{col_name}",
                label_visibility="collapsed"
            )
            # Update session state
            st.session_state.column_config[col_name]["friendly_name"] = new_friendly

        with col3:
            new_visible = st.checkbox(
                "visible",
                value=st.session_state.column_config[col_name]["is_visible"],
                key=f"visible_{idx}_{col_name}",
                label_visibility="collapsed"
            )
            st.session_state.column_config[col_name]["is_visible"] = new_visible

        with col4:
            new_pii = st.checkbox(
                "pii",
                value=st.session_state.column_config[col_name]["contains_pii"],
                key=f"pii_{idx}_{col_name}",
                label_visibility="collapsed"
            )
            st.session_state.column_config[col_name]["contains_pii"] = new_pii

    st.markdown("---")

    # Save/Cancel buttons
    col1, col2 = st.columns([1, 4])

    with col1:
        if st.button("💾 Save Configuration", type="primary"):
            if not table_friendly_name:
                st.error("❌ Please provide a friendly name for the table")
            else:
                with st.spinner("Saving configuration..."):
                    # Build columns payload
                    columns_payload = []
                    for col_name, config in st.session_state.column_config.items():
                        orig_col = next((c for c in columns if (c['column_name'] if isinstance(c, dict) else c.column_name) == col_name), None)
                        columns_payload.append({
                            'column_name': col_name,
                            'friendly_name': config['friendly_name'],
                            'is_visible': config['is_visible'],
                            'data_type': orig_col.get('data_type', 'VARCHAR') if isinstance(orig_col, dict) else 'VARCHAR',
                            'contains_pii': config.get('contains_pii', False),
                            'data_classification': config.get('data_classification', 'INTERNAL')
                        })

                    # Build full payload
                    payload = {
                        'system_id': system_id,
                        'table_name': table_to_config,
                        'table_friendly_name': table_friendly_name,
                        'table_description': table_description,
                        'contains_pii': contains_pii,
                        'gdpr_category': gdpr_category if gdpr_category != 'None' else None,
                        'data_sensitivity': data_sensitivity,
                        'columns': columns_payload,
                        'relationships': [],
                        'created_by': st.session_state.username
                    }

                    try:
                        response = httpx.post(
                            f"{GATEWAY_URL}/api/metadata/save-extended",
                            json=payload,
                            timeout=30.0
                        )

                        if response.status_code == 200:
                            result = response.json()
                            st.success(f"✅ {result['message']}")
                            invalidate_cache(system_id)

                            # Clear session state
                            st.session_state.selected_table = None
                            st.session_state.editing_table = None
                            st.session_state.column_config = {}

                            st.balloons()
                            st.rerun()
                        else:
                            st.error(f"❌ Failed to save: {response.text}")

                    except Exception as e:
                        st.error(f"❌ Error saving configuration: {e}")

    with col2:
        if st.button("❌ Cancel"):
            st.session_state.selected_table = None
            st.session_state.editing_table = None
            st.session_state.column_config = {}
            st.rerun()
