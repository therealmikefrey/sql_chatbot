"""
Initialize the SQL Chatbot
"""
from langchain_community.llms import Ollama

from sql_analyzer.config import cfg
from sql_analyzer.log_init import logger
from sql_analyzer.sql_chatbot import SQLChatbot
from sql_analyzer.sql_db_factory import sql_db_factory


def init_chatbot() -> SQLChatbot:
    db = sql_db_factory()
    llm = Ollama(base_url=cfg.ollama_url, model=cfg.ollama_model)
    return SQLChatbot(db, llm)


if __name__ == "__main__":
    chatbot = init_chatbot()
    logger.info("Available tables: %s", chatbot.get_table_names())
    
    print("\nWelcome to SQL Chatbot! Type 'exit' to quit.")
    print("Available tables:", ", ".join(chatbot.get_table_names()))
    
    while True:
        try:
            question = input("\nWhat would you like to know? ").strip()
            if question.lower() in ['exit', 'quit']:
                break
                
            if not question:
                continue
                
            print("\nThinking...")
            response = chatbot.answer_question(question)
            print("\nAnswer:", response)
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"\nError: {str(e)}")
    
    print("\nGoodbye!")
