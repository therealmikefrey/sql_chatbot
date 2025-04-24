from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import traceback
from urllib.parse import parse_qs, urlparse
from sql_analyzer.agent_factory import init_chatbot
from sql_analyzer.log_init import logger

# Initialize chatbot
chatbot = init_chatbot()

class ChatbotHandler(BaseHTTPRequestHandler):
    def _set_response(self, status_code=200, content_type="application/json"):
        self.send_response(status_code)
        self.send_header('Content-type', content_type)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def _send_json_response(self, data, status_code=200):
        try:
            self._set_response(status_code)
            response = json.dumps(data).encode('utf-8')
            self.wfile.write(response)
        except (BrokenPipeError, ConnectionResetError):
            # Client disconnected, nothing we can do
            pass
        except Exception as e:
            # Log the error but don't try to send it since the connection might be dead
            logger.error(f"Error sending response: {str(e)}")
    
    def do_OPTIONS(self):
        self._set_response()
        
    def do_GET(self):
        """Handle GET requests - used for getting tables."""
        if self.path == "/tables":
            try:
                tables = chatbot.get_table_names()
                self._send_json_response({"tables": tables})
            except Exception as e:
                logger.error(f"Error getting tables: {str(e)}\n{traceback.format_exc()}")
                self._send_json_response({"error": str(e)}, 500)
        else:
            self._send_json_response({"error": "Not found"}, 404)
    
    def do_POST(self):
        """Handle POST requests - used for processing queries."""
        if self.path == "/query":
            try:
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode())
                
                # Log the incoming question
                logger.info(f"Processing question: {data['text']}")
                
                # Extract table name from question
                table_name = chatbot.extract_table_name(data["text"])
                logger.info(f"Extracted table name: {table_name}")
                
                # Get schema
                schema = chatbot.get_schema(table_name)
                logger.info(f"Retrieved schema for table {table_name}")
                
                # Generate SQL
                sql = chatbot.generate_sql(data["text"], schema)
                logger.info(f"Generated SQL: {sql}")
                
                # Execute query
                result = chatbot.execute_query(sql)
                logger.info(f"Query executed successfully")
                
                # Format response
                response = chatbot.format_response(data["text"], result, sql)
                logger.info("Response formatted")
                
                self._send_json_response({
                    "sql": sql,
                    "response": response
                })
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON: {str(e)}")
                self._send_json_response({"error": "Invalid JSON"}, 400)
            except KeyError as e:
                logger.error(f"Missing field: {str(e)}")
                self._send_json_response({"error": "Missing required field 'text'"}, 400)
            except Exception as e:
                logger.error(f"Error processing query: {str(e)}\n{traceback.format_exc()}")
                self._send_json_response({"error": str(e)}, 500)
        else:
            self._send_json_response({"error": "Not found"}, 404)

def run(port=8000):
    server_address = ('', port)
    httpd = HTTPServer(server_address, ChatbotHandler)
    logger.info(f"Starting server on port {port}...")
    httpd.serve_forever()

if __name__ == "__main__":
    run()
