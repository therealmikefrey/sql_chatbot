import chainlit as cl
import httpx
from httpx import TimeoutException, ConnectError

API_URL = "http://localhost:8000"  # FastAPI server URL

# Configure timeouts
TIMEOUTS = httpx.Timeout(
    timeout=300.0,  # Default timeout for all operations
    connect=30.0,   # Connection timeout
    read=300.0,     # Read timeout
    write=60.0,     # Write timeout
    pool=None       # Pool timeout (None means no pool timeout)
)

# Initialize chatbot at startup, not module level
chatbot = None

@cl.on_chat_start
async def start():
    """Send a welcome message when the chat starts."""
    try:
        # Get tables from API
        async with httpx.AsyncClient(timeout=TIMEOUTS) as client:
            response = await client.get(f"{API_URL}/tables")
            data = response.json()
            tables = data["tables"]
        
        await cl.Message(
            content=f"Welcome to SQL Chatbot! I can help you query your database.\n\nAvailable tables:\n" + 
                    "\n".join(f"- {table}" for table in tables)
        ).send()
    except Exception as e:
        await cl.Message(
            content="I apologize, but I couldn't connect to the database server. Please make sure it's running."
        ).send()

@cl.on_message
async def main(message: cl.Message):
    """Process each message from the user."""
    # Let user know we're working
    thinking_msg = cl.Message(content="Thinking...")
    await thinking_msg.send()
    
    try:
        # Send question to API with timeout
        async with httpx.AsyncClient(timeout=TIMEOUTS) as client:
            response = await client.post(
                f"{API_URL}/query",
                json={"text": message.content}
            )
            
            # Check if the request was successful
            response.raise_for_status()
            
            # Parse the response
            data = response.json()
            
            # Show the SQL query
            await cl.Message(content=f"```sql\n{data['sql']}\n```").send()
            
            # Send the final answer
            await thinking_msg.remove()
            await cl.Message(content=data["response"]).send()
            
    except TimeoutException:
        await thinking_msg.remove()
        await cl.Message(
            content="I apologize, but the request timed out. Please try again or try a simpler query."
        ).send()
    except ConnectError:
        await thinking_msg.remove()
        await cl.Message(
            content="I apologize, but I couldn't connect to the server. Please make sure both the API and database servers are running."
        ).send()
    except Exception as e:
        await thinking_msg.remove()
        await cl.Message(
            content=f"I apologize, but I encountered an error: {str(e)}"
        ).send()
