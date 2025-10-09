import streamlit as st
import asyncio
import httpx
import os
from openai import OpenAI
from dotenv import load_dotenv
from datetime import datetime, timedelta
from langchain_community.chat_message_histories import StreamlitChatMessageHistory
from langchain.memory import ConversationBufferMemory
from langchain.schema import HumanMessage, AIMessage
import json
import os
import uuid
from datetime import datetime
from utils.text_to_sql_converter import generate_sql, ROLE_PROMPTS
load_dotenv()

st.set_page_config(
    page_title="Förlagssystem AI Assistant",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Company colors and styling
st.markdown("""
<style>
    :root {
        --primary-color: #0073AE;
        --background-color: #f2f4f8;
        --secondary-bg: #0F2436;
        --text-color: #0F2436;
    }

    .main {
        background-color: #f2f4f8;
    }

    .stButton>button {
        background-color: #0073AE;
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

    .user-message {
        background-color: #0073AE;
        color: white;
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
        max-width: 80%;
        margin-left: auto;
    }

    .assistant-message {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
        max-width: 80%;
        border-left: 4px solid #0073AE;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }

    h1 {
        color: #0F2436;
        font-weight: 700;
    }

    .stTextInput>div>div>input {
        border-radius: 25px;
        border: 2px solid #0073AE;
        padding: 12px 20px;
    }

    button[kind="formSubmit"] {
        display: none;
    }
</style>
""", unsafe_allow_html=True)

# Configuration
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
GATEWAY_URL = os.getenv("GATEWAY_URL")
GATEWAY_TOKEN = os.getenv("GATEWAY_TOKEN")

# Database memory services via FastAPI
class DatabaseMemoryClient:
    """Client for database memory operations via FastAPI"""
    
    def __init__(self):
        self.gateway_url = GATEWAY_URL
        self.gateway_token = GATEWAY_TOKEN
        
    async def create_session(self, session_id: str, user_id: str, metadata: dict = None):
        """Create session via API"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.gateway_url}/api/conversation/create-session",
                    json={
                        "session_id": session_id,
                        "user_id": user_id,
                        "metadata": metadata or {}
                    },
                    headers={"Authorization": f"Bearer {self.gateway_token}"},
                    timeout=10.0
                )
                result = response.json()
                return result.get("success", False)
        except Exception as e:
            st.sidebar.error(f"Create session API error: {str(e)}")
            return False
            
    async def save_message(self, session_id: str, message_type: str, message_content: str, message_metadata: str = None):
        """Save message via API"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.gateway_url}/api/conversation/save-message",
                    json={
                        "session_id": session_id,
                        "message_type": message_type,
                        "message_content": message_content,
                        "message_metadata": message_metadata
                    },
                    headers={"Authorization": f"Bearer {self.gateway_token}"},
                    timeout=10.0
                )
                result = response.json()
                return result.get("success", False)
        except Exception as e:
            st.sidebar.error(f"Save message API error: {str(e)}")
            return False
            
    async def get_session_messages(self, session_id: str):
        """Get session messages via API"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.gateway_url}/api/conversation/get-messages/{session_id}",
                    headers={"Authorization": f"Bearer {self.gateway_token}"},
                    timeout=10.0
                )
                result = response.json()
                return result.get("messages", [])
        except Exception as e:
            st.sidebar.error(f"Get messages API error: {str(e)}")
            return []
            
    async def update_conversation_context(self, session_id: str, last_query: str, last_sql: str = None, 
                                        last_tables_used: str = None, result_count: int = 0):
        """Update conversation context via API"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.gateway_url}/api/conversation/update-context",
                    json={
                        "session_id": session_id,
                        "last_query": last_query,
                        "last_sql": last_sql,
                        "last_tables_used": last_tables_used,
                        "result_count": result_count
                    },
                    headers={"Authorization": f"Bearer {self.gateway_token}"},
                    timeout=10.0
                )
                result = response.json()
                return result.get("success", False)
        except Exception as e:
            st.sidebar.error(f"Update context API error: {str(e)}")
            return False
            
    async def clear_session_messages(self, session_id: str):
        """Clear session messages via API"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.delete(
                    f"{self.gateway_url}/api/conversation/clear-session/{session_id}",
                    headers={"Authorization": f"Bearer {self.gateway_token}"},
                    timeout=10.0
                )
                result = response.json()
                return result.get("success", False)
        except Exception as e:
            st.sidebar.error(f"Clear session API error: {str(e)}")
            return False

# Initialize API client
@st.cache_resource
def get_database_memory_client():
    """Initialize database memory API client"""
    return DatabaseMemoryClient()

db_memory_service = get_database_memory_client()
conversation_memory_service = None  # Not needed anymore


# Get current date for queries
TODAY = datetime(2025, 9, 10)
TODAY_STR = TODAY.strftime("%Y%m%d")

# Calculate week boundaries
WEEK_START = (TODAY - timedelta(days=TODAY.weekday())).strftime("%Y%m%d")
WEEK_END = (TODAY + timedelta(days=6-TODAY.weekday())).strftime("%Y%m%d")
MONTH_START = TODAY.replace(day=1).strftime("%Y-%m-%d")
next_month = (TODAY.replace(day=28) + timedelta(days=4)).replace(day=1)
MONTH_END = (next_month - timedelta(days=1)).strftime("%Y-%m-%d")

# Last month calculation
last_month_end = TODAY.replace(day=1) - timedelta(days=1)
LAST_MONTH_END = last_month_end.strftime("%Y%m%d")
LAST_MONTH_START = last_month_end.replace(day=1).strftime("%Y%m%d")

# Year start
YEAR_START = TODAY.replace(month=1, day=1).strftime("%Y%m%d")


def initialize_conversation_memory():
    """Initialize conversation memory for the session"""
    if "conversation_memory" not in st.session_state:
        st.session_state.conversation_memory = ConversationBufferMemory(
            chat_memory=StreamlitChatMessageHistory(key="chat_messages"),
            return_messages=True,
            memory_key="chat_history",
            input_key="input",
            output_key="output"
        )
    return st.session_state.conversation_memory

def initialize_chat_session():
    """Initialize or restore chat session with database integration and proper session persistence"""
    
    # Always ensure we have a session_id and user_id
    if 'session_id' not in st.session_state or not st.session_state.session_id:
        st.session_state.session_id = str(uuid.uuid4())
        print(f"Created new session: {st.session_state.session_id}")
    else:
        print(f"Using existing session: {st.session_state.session_id}")
    
    # Ensure user_id is set
    if 'user_id' not in st.session_state:
        st.session_state.user_id = st.session_state.username if st.session_state.username else "default_user"
    
    # Skip database operations if service is not available
    if not db_memory_service:
        st.warning("⚠️ Database memory service not available - using session memory only")
        if 'messages' not in st.session_state:
            st.session_state.messages = []
        return
    
    # Initialize database session
    try:
        # Check if session already exists, if not create it
        try:
            # Try to load existing messages first to see if session exists
            existing_messages = asyncio.run(db_memory_service.get_session_messages(st.session_state.session_id))
            print(f"Found existing session with {len(existing_messages)} messages")
            
            # Convert database messages to Streamlit format
            if existing_messages and len(existing_messages) > 0:
                st.session_state.messages = []
                for msg in existing_messages:
                    st.session_state.messages.append({
                        "role": msg['message_type'],
                        "content": msg['message_content'],
                        "timestamp": msg.get('timestamp', '')
                    })
                st.sidebar.info(f"📚 Loaded {len(existing_messages)} messages from database")
            else:
                # No existing messages, ensure we have empty list
                if 'messages' not in st.session_state:
                    st.session_state.messages = []
            
        except Exception as load_error:
            print(f"Session doesn't exist or load failed: {load_error}")
            
            # Create new session in database
            session_created = asyncio.run(db_memory_service.create_session(
                session_id=st.session_state.session_id,
                user_id=st.session_state.user_id,
                metadata={
                    "created": datetime.now().isoformat(),
                    "username": st.session_state.username
                }
            ))
            
            if session_created:
                st.sidebar.success("✅ New database session created")
                print(f"Created new database session: {st.session_state.session_id}")
            else:
                st.sidebar.warning("⚠️ Could not create database session")
            
            # Initialize empty messages
            if 'messages' not in st.session_state:
                st.session_state.messages = []
                
    except Exception as e:
        st.sidebar.error(f"❌ Database initialization error: {str(e)}")
        print(f"Database initialization error: {e}")
        
        # Fall back to session-only memory
        if 'messages' not in st.session_state:
            st.session_state.messages = []
    
    # Ensure conversation memory is initialized
    initialize_conversation_memory()
    
    print(f"Session initialized: {st.session_state.session_id}")
    print(f"Messages count: {len(st.session_state.messages)}")


def save_message_to_database(role: str, content: str, metadata: dict = None):
    """Save message to database with error handling"""
    
    # Skip if database service not available
    if not db_memory_service:
        return False
        
    # Skip if no session_id
    if 'session_id' not in st.session_state:
        return False
    
    try:
        success = asyncio.run(db_memory_service.save_message(
            session_id=st.session_state.session_id,
            message_type=role,
            message_content=content,
            message_metadata=json.dumps(metadata) if metadata else None
        ))
        
        if not success:
            st.sidebar.error("⚠️ Failed to save message to database")
            return False
        
        # Update session activity
        return True
        
    except Exception as e:
        st.sidebar.error(f"❌ Database save error: {str(e)}")
        return False

def update_conversation_context(user_question: str, generated_sql: str, tables_used: str = "", result_count: int = 0):
    """Update conversation context in database"""
    
    # Skip if database service not available
    if not db_memory_service or 'session_id' not in st.session_state:
        return False
    
    try:
        success = asyncio.run(db_memory_service.update_conversation_context(
            session_id=st.session_state.session_id,
            last_query=user_question,
            last_sql=generated_sql,
            last_tables_used=tables_used,
            result_count=result_count
        ))
        return success
    except Exception as e:
        st.sidebar.error(f"⚠️ Context update error: {str(e)}")
        return False


# Session state
if 'messages' not in st.session_state:
    st.session_state.messages = []
if 'username' not in st.session_state:
    st.session_state.username = None

async def execute_query(sql: str, username: str):
    async with httpx.AsyncClient() as http_client:
        response = await http_client.post(
            f"{GATEWAY_URL}/api/execute-query",
            json={"query": sql},
            headers={
                "Authorization": f"Bearer {GATEWAY_TOKEN}",
                "X-Username": username
            },
            timeout=30.0
        )
        return response.json()


def handle_sql_generation(question: str, username: str):
    """Generate SQL with database conversation context"""
    from utils.text_to_sql_converter import generate_sql_with_session_context
    
    session_id = st.session_state.get('session_id', None)
    return generate_sql_with_session_context(question, username, session_id)

def format_results(question: str, rows: list, username: str) -> str:
    """Format results based on role - NO unnecessary suggestions"""
    
    format_prompt = f"""User asked: "{question}"

Database results ({len(rows)} rows):
{rows}

Instructions:
1. Present the data clearly and professionally
2. Format numbers: 1,234,567 SEK for money, dates as readable text
3. Lead with the key answer or total
4. Be comprehensive but concise
5. DO NOT add suggestions, next steps, or additional analysis
6. Just present the facts directly and professionally
7. If dates are in YYYYMMDD format, convert to readable: 20251006 → October 6, 2025

Present the answer now:"""

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": ROLE_PROMPTS[username]},
            {"role": "user", "content": format_prompt}
        ],
        temperature=0.3
    )
    return response.choices[0].message.content

# Sidebar
with st.sidebar:
    st.image("https://www.forlagssystem.se/wp-content/uploads/2023/02/forlagssystem_logo_white.svg",
             width=True)
    st.markdown("---")

    if st.session_state.username is None:
        st.markdown("### Login")
        username = st.selectbox(
            "Select your account:",
            ["", "harold", "lars", "pontus", "peter", "linda"],
            format_func=lambda x: {
                "": "-- Select User --",
                "harold": "Harold (CEO)",
                "lars": "Lars (Finance)",
                "pontus": "Pontus (Call Center)",
                "peter": "Peter (Logistics)",
                "linda": "Linda (Customer Service)"
            }[x]
        )

        if st.button("Login", width=True):
            if username:
                st.session_state.username = username
                st.session_state.messages = []
                initialize_chat_session()  # ADD THIS LINE
                st.rerun()
    else:
        st.markdown(f"### Logged in as")
        st.markdown(f"**{st.session_state.username.upper()}**")
        
        # Show current date context
        st.markdown("---")
        st.markdown(f"**Today:** {TODAY.strftime('%B %d, %Y')}")
        st.markdown(f"**This Week:** {datetime.strptime(WEEK_START, '%Y%m%d').strftime('%b %d')} - {datetime.strptime(WEEK_END, '%Y%m%d').strftime('%b %d')}")

        if st.button("Logout", width=True):
            st.session_state.username = None
            st.session_state.messages = []
            st.rerun()

        # ADD THIS NEW SECTION:
        st.markdown("---")
        st.markdown("### 🧠 Memory Controls")

        # Show session info
        if 'session_id' in st.session_state:
            st.info(f"**Session:** {st.session_state.session_id[:8]}...")

        # Memory control buttons
        col1, col2 = st.columns(2)

        with col1:
            if st.button("🗑️ Clear Session", width=True):
                try:
                    # Clear from database
                    if db_memory_service and 'session_id' in st.session_state:
                        success = asyncio.run(db_memory_service.clear_session_messages(st.session_state.session_id))
                        if success:
                            st.success("✅ DB cleared")
                        else:
                            st.error("❌ DB clear failed")
                    
                    # Clear local memory
                    if "conversation_memory" in st.session_state:
                        st.session_state.conversation_memory.clear()
                    if "messages" in st.session_state:
                        st.session_state.messages = []
                    st.rerun()
                except Exception as e:
                    st.error(f"Clear error: {str(e)}")

        with col2:
            if st.button("📱 New Session", width=True):
                try:
                    # Start new session
                    st.session_state.session_id = str(uuid.uuid4())
                    st.session_state.messages = []
                    if "conversation_memory" in st.session_state:
                        st.session_state.conversation_memory.clear()
                    initialize_chat_session()
                    st.success("✅ New session")
                    st.rerun()
                except Exception as e:
                    st.error(f"New session error: {str(e)}")

        # Show memory stats
        try:
            if db_memory_service and 'session_id' in st.session_state:
                message_count = len(asyncio.run(db_memory_service.get_session_messages(st.session_state.session_id)))
                st.metric("DB Messages", message_count)
            
            if "conversation_memory" in st.session_state:
                memory_vars = st.session_state.conversation_memory.load_memory_variables({})
                chat_history = memory_vars.get("chat_history", [])
                st.metric("Memory Turns", len(chat_history))
        except Exception as e:
            st.error(f"Stats error: {str(e)}")



# Main content
if st.session_state.username is None:
    col1, col2, col3 = st.columns([1,2,1])
    with col2:
        st.image("https://www.forlagssystem.se/wp-content/uploads/2023/02/forlagssystem_logo.svg",
                 width=True)
        st.markdown("<h1 style='text-align: center;'>AI Assistant</h1>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: center; color: #666;'>Select your account to get started</p>",
                   unsafe_allow_html=True)

else:
    col1, col2 = st.columns([1, 4])
    with col1:
        st.image("https://www.forlagssystem.se/wp-content/uploads/2023/02/forlagssystem_logo.svg",
                 width=150)
    with col2:
        st.markdown(f"<h1>Chat with AI Assistant</h1>", unsafe_allow_html=True)
        st.markdown(f"Hi **{st.session_state.username.upper()}**, I'm your assistant today. I can help you with STYR data.")

    st.markdown("---")

    # Display chat history
    for msg in st.session_state.messages:
        if msg["role"] == "user":
            st.markdown(f"<div class='user-message'>{msg['content']}</div>", unsafe_allow_html=True)
        else:
            st.markdown(f"<div class='assistant-message'>{msg['content']}</div>", unsafe_allow_html=True)

    # Input form
    with st.form(key='chat_form', clear_on_submit=True):
        user_input = st.text_input(
            "Ask me anything:",
            placeholder="e.g., Show me this week's sales report",
            label_visibility="collapsed"
        )
        submit = st.form_submit_button("Send")

    if submit and user_input:
        # Initialize memory
        memory = initialize_conversation_memory()
        
        st.session_state.messages.append({"role": "user", "content": user_input})
        save_message_to_database("user", user_input) 
        print("DEBUG: User question =", user_input)
        with st.spinner("Analyzing..."):
            try:
                # Generate SQL with conversation context
                sql = generate_sql(user_input, st.session_state.username)
                print("GENERATED SQL:", sql)
                print(f"DEBUG: Generated SQL = {sql}")
                if not sql.strip().upper().startswith("SELECT"):
                    response = "I can only retrieve information from the system; I can't perform any other operations at the moment."
                else:
                    result = asyncio.run(execute_query(sql, st.session_state.username))
                    if result.get("success"):
                        rows = result["data"]["rows"]
                        if len(rows) == 0:
                            response = "No data found matching your query."
                        else:
                            response = format_results(user_input, rows, st.session_state.username)
                            
                            # Save to conversation memory
                            memory.save_context(
                                {"input": user_input},
                                {"output": response}
                            )
                            # UPDATE CONVERSATION CONTEXT IN DATABASE - ADD THIS BLOCK
                            if sql and sql.strip().upper().startswith("SELECT"):
                                # Extract table names from SQL (simple extraction)
                                tables_used = ""
                                try:
                                    if "FROM" in sql.upper():
                                        sql_parts = sql.upper().split("FROM")[1].split("WHERE")[0].split("ORDER")[0].split("GROUP")[0]
                                        tables_used = sql_parts.strip()[:200]  # Limit length
                                except:
                                    tables_used = "UNKNOWN"
                                
                                result_count = len(rows) if rows else 0
                                update_conversation_context(user_input, sql, tables_used, result_count)
                            # END OF ADDED BLOCK
                    else:
                        error_msg = result.get("message", "").lower()
                        if "permission" in error_msg or "access" in error_msg or "denied" in error_msg:
                            response = "You don't have permission to access that information."
                        else:
                            response = f"Query error. Please try rephrasing your question."
                            
                st.session_state.messages.append({"role": "assistant", "content": response})
                save_message_to_database("assistant", response)
                st.rerun()
            except Exception as e:
                response = "An error occurred. Please try again."
                st.session_state.messages.append({"role": "assistant", "content": response})
                st.rerun()
