from typing import List, Tuple, Any, Union
from langchain_community.utilities.sql_database import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain.agents import Tool, AgentExecutor, LLMSingleActionAgent
from langchain.schema import AgentAction, AgentFinish
from langchain.prompts import StringPromptTemplate
from langchain_community.llms import Ollama
from langchain.chains.llm import LLMChain
from langchain.agents.agent import AgentOutputParser
from langchain.tools.base import BaseTool
import re

from sql_analyzer.config import cfg
from sql_analyzer.log_init import logger
from sql_analyzer.sql.sql_tool import ExtendedSQLDatabaseToolkit
from sql_analyzer.sql_db_factory import sql_db_factory


template = """You are a helpful SQL assistant that helps users understand their database.
You have access to the following tools:

{tools}

You MUST follow this EXACT format with NO DEVIATIONS:

Question: <the input question>
Thought: <your reasoning>
Action: <one of: sql_db_query, sql_db_schema, sql_db_list_tables>
Action Input: <your input to the action>
Observation: <the result>
Thought: I now know the final answer
Final Answer: <your answer>

Rules:
1. After checking schema, use sql_db_query (not sql_count) to count records
2. For sql_db_query, Action Input must be a complete SQL query
3. Put ALL explanations in the Final Answer section
4. The schema output shows only SAMPLE rows - to get accurate counts you must run sql_db_query
5. Always trust the results of sql_db_query over any sample data
6. NO markdown formatting (```) in the Action Input
7. NO explanations between steps - put them all in Final Answer
8. For sql_db_schema, do not put quotes around table names
9. After confirming a table exists with sql_db_list_tables, use sql_db_schema to check its structure
10. NEVER put quotes around SQL queries in Action Input - write them directly
11. After getting a successful query result, ALWAYS proceed to Final Answer

Example:
Question: How many users are active?
Thought: I need to count active users
Action: sql_db_query
Action Input: SELECT COUNT(*) FROM users WHERE active = 1
Observation: [(42,)]
Thought: I now know the final answer
Final Answer: There are 42 active users in the system.

Question: {input}
{agent_scratchpad}"""


class CustomPromptTemplate(StringPromptTemplate):
    template: str
    tools: List[BaseTool]
    
    @property
    def _tools_names(self) -> List[str]:
        return [tool.name for tool in self.tools]
    
    @property
    def _tools_descriptions(self) -> str:
        return "\n".join([f"{tool.name}: {tool.description}" for tool in self.tools])

    def format(self, **kwargs) -> str:
        intermediate_steps = kwargs.pop("intermediate_steps")
        thoughts = ""
        for action, observation in intermediate_steps:
            thoughts += action.log
            thoughts += f"\nObservation: {observation}\n"
        kwargs["agent_scratchpad"] = thoughts
        kwargs["tools"] = self._tools_descriptions
        kwargs["tool_names"] = ", ".join(self._tools_names)
        return self.template.format(**kwargs)


class CustomOutputParser(AgentOutputParser):
    def parse(self, llm_output: str) -> Union[AgentAction, AgentFinish]:
        if "Final Answer:" in llm_output:
            return AgentFinish(
                return_values={"output": llm_output.split("Final Answer:")[-1].strip()},
                log=llm_output,
            )

        # Look for action and action input with more flexible regex
        regex = r"Action:\s*([^\n]*?)\s*[\n]*Action Input:\s*(.*?)(?=\n*(?:Observation|$))"
        match = re.search(regex, llm_output, re.DOTALL)
        
        if not match:
            raise ValueError(f"Could not parse LLM output: `{llm_output}`")
        
        action = match.group(1).strip()
        action_input = match.group(2).strip()
        
        # Remove any markdown code formatting and quotes
        action_input = action_input.strip('`').strip('"\'').strip()
        if action_input.startswith('sql\n'):
            action_input = action_input[4:]
        
        return AgentAction(tool=action, tool_input=action_input, log=llm_output)


def init_sql_db_toolkit() -> SQLDatabaseToolkit:
    db: SQLDatabase = sql_db_factory()
    llm = Ollama(base_url=cfg.ollama_url, model=cfg.ollama_model)
    toolkit = ExtendedSQLDatabaseToolkit(db=db, llm=llm)
    return toolkit


def initialize_agent(toolkit: SQLDatabaseToolkit) -> AgentExecutor:
    llm = Ollama(base_url=cfg.ollama_url, model=cfg.ollama_model)
    tools = toolkit.get_tools()
    
    prompt = CustomPromptTemplate(
        template=template,
        tools=tools,
        input_variables=["input", "intermediate_steps"]
    )
    
    output_parser = CustomOutputParser()
    
    llm_chain = LLMChain(llm=llm, prompt=prompt)
    
    agent = LLMSingleActionAgent(
        llm_chain=llm_chain,
        output_parser=output_parser,
        stop=["\nObservation:"],
        allowed_tools=[tool.name for tool in tools],
    )
    
    return AgentExecutor.from_agent_and_tools(
        agent=agent,
        tools=tools,
        verbose=True,
        max_iterations=5,
        handle_parsing_errors=True,
    )


def agent_factory() -> AgentExecutor:
    sql_db_toolkit = init_sql_db_toolkit()
    agent_executor = initialize_agent(sql_db_toolkit)
    return agent_executor


if __name__ == "__main__":
    agent_executor = agent_factory()
    query = "How many records in the Prj_Data_Transfers_SC table are marked as 'Y' for Recibido?"
    result = agent_executor.invoke({"input": query})
    logger.info("Query: %s", query)
    logger.info("Result: %s", result)
