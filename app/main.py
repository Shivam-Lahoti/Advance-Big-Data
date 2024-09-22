from src import create_app
import threading
from src.queue_service import start_consumer_thread
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app= create_app()
if __name__=="__main__":
    

    def process_message(message):
        logger.info(f"--------------->Processing message: {message}")

 
    start_consumer_thread(process_message)

    app.run(debug=True)