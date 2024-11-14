import logging
import azure.functions as func
from .main import main as main_func

def main(req: func.HttpRequest, inputBlob: bytes) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    return main_func(req, inputBlob)

