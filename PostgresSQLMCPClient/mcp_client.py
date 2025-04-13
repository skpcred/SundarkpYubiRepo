from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from langchain_openai import ChatOpenAI
from langchain_mcp_adapters.tools import load_mcp_tools
from langgraph.prebuilt import create_react_agent
from dotenv import load_dotenv
import os
import asyncio

# Load environment variables
load_dotenv()

# Get environment variables
openai_model_name = os.getenv("OPENAI_MODEL_NAME")
openai_api_base = os.getenv("OPENAI_API_BASE")
openai_api_key = os.getenv("OPENAI_API_KEY")

# Verify environment variables
if not all([openai_model_name, openai_api_base, openai_api_key]):
    raise ValueError("Missing required environment variables. Please check your .env file.")

# Configure MCP server parameters
server_params = StdioServerParameters(
    command="python",
    args=["/Users/sundara.padmanabhan/Documents/Code/mcps/legion-mcp-server/src/database_mcp/mcp_server.py"],
)

# Create LLM configuration
model = ChatOpenAI(
    model_name=openai_model_name,
    base_url=openai_api_base,
    api_key=openai_api_key,
    temperature=0.7
)

async def run_agent():
  async with stdio_client(server_params) as (read, write):
    # Open an MCP session to interact with the math_server.py tool.
    async with ClientSession(read, write) as session:
      # Initialize the session.
      await session.initialize()
      # Load tools
      tools = await load_mcp_tools(session)
      #print(tools)

      # Create a ReAct agent.
      agent = create_react_agent(model, tools)
      # Run the agent.
      agent_response = await agent.ainvoke(
      # Now, let's give our message.
       {"messages": "Please give the list of tables"})
      # Return the response.
      messages =  agent_response["messages"]
      return messages
      

if __name__ == "__main__":
  result = asyncio.run(run_agent())
  length = len(result)
  print (result[length-1])
