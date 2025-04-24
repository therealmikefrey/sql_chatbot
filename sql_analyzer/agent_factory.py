"""
SQL Chatbot factory module.
"""
from langchain_community.utilities.sql_database import SQLDatabase
from langchain_community.llms import Ollama

from sql_analyzer.config import cfg
from sql_analyzer.log_init import logger
from sql_analyzer.sql_db_factory import sql_db_factory
from sql_analyzer.sql_chatbot import SQLChatbot


def init_chatbot() -> SQLChatbot:
    """Initialize the SQL chatbot with database connection and LLM."""
    db: SQLDatabase = sql_db_factory()
    llm = Ollama(base_url=cfg.ollama_url, model=cfg.ollama_model)
    return SQLChatbot(db=db, llm=llm)


if __name__ == "__main__":
    chatbot = init_chatbot()
    query = "How many records in the Prj_Data_Transfers_SC table are marked as 'Y' for Recibido?"
    result = chatbot.answer_question(query)
    logger.info("Query: %s", query)
    logger.info("Result: %s", result)
