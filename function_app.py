import azure.functions as func
import logging, os, json, requests
from datetime import datetime
from requests.auth import HTTPDigestAuth
import asyncio
from azure.storage.blob import BlobServiceClient
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="PhotoCapture")
def PhotoCapture(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Running Photo Capture Function...')

    try:
        req_body_bytes = req.get_body()
        req_body = req_body_bytes.decode("utf-8")
        settings = os.environ.get("TOC_CONFIGURATION")

        if settings is not None:
            vFilename = "PIC_" + datetime.today().strftime('%Y%m%d%H%M%S%f') + ".JPG"
            config  = eval(settings)
            webURL = config["CameraUrl"]
            userCam = config["CameraAuth"]
            secretCam = config["CameraSecret"]
            response = requests.get(webURL, auth=HTTPDigestAuth(userCam, secretCam), stream=True, verify=False)
            img = response.raw

            dataEventHub = config
            dataEventHub["PicName"] = vFilename
            dataEventHub["DateTime"] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
            jsonstr = json.dumps(dataEventHub) 
            logging.info(f"Message Event Hub: {jsonstr}")

            ##ENVIANDO ARCHIVO AL STORAGE
            asyncio.run(UploadFileAsync(vFilename, img))

            ##ENVIANDO MENSAJE A EVENT HUB
            asyncio.run(SendMessageEventHub(jsonstr))

            return func.HttpResponse(f"FILENAME: {vFilename}")
        else:
            return func.HttpResponse(f"ERROR: No hay configuración disponible")
    except Exception as ex:
        logging.error('An error ocurred: %s', repr(ex))
        return func.HttpResponse("Function error ocurred.", status_code=200)

async def UploadFileAsync(fileName, content):
    try:
        blobContainer = os.environ.get("TOC_STORAGE_CONTAINER")
        blobConnection = os.environ.get("TOC_STORAGE_CONNECTION")
        blobClient = BlobServiceClient.from_connection_string(blobConnection)
        blobStorageClient = blobClient.get_blob_client(container=blobContainer, blob=fileName)

        # Upload the created file
        blobStorageClient.upload_blob(content)

    except Exception as ex:
        logging.error('An error ocurred: %s', repr(ex))

async def SendMessageEventHub(msg):
    try:
        TOC_EVENT_HUB_CONNECTION = os.environ.get("TOC_EVENT_HUB_CONNECTION")
        TOC_EVENT_HUB_NAME = os.environ.get("TOC_EVENT_HUB_NAME")
        producerClient = EventHubProducerClient.from_connection_string(conn_str=TOC_EVENT_HUB_CONNECTION, eventhub_name=TOC_EVENT_HUB_NAME)

        async with producerClient:
            # Create a batch.
            event_data_batch = await producerClient.create_batch()

            # Add events to the batch.
            event_data_batch.add(EventData(msg))
            logging.info(f"Message sent: {msg}")

            # Send the batch of events to the event hub.
            await producerClient.send_batch(event_data_batch)
    except Exception as ex:
        logging.error('An error ocurred: %s', repr(ex))