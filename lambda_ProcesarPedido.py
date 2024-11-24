import json
import boto3

def lambda_handler(event, context):
    # Crear cliente SQS
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/630606600000/sqs-pedidos'

    # Obtener despachador_id del evento
    despachador_id = event["despachador_id"]  # se obtiene el despachador_id

    # Recibir mensajes de la cola SQS
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=3,
        WaitTimeSeconds=10
    )
    
    print(response)
    messages = response.get('Messages', [])
    pedidos = []
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('t_pedidos_procesados')

    for message in messages:
        pedido = json.loads(message['Body'])
        print(pedido)

        # Agregar despachador_id al pedido
        pedido['despachador_id'] = despachador_id

        # Almacenar el pedido en DynamoDB
        response_dynamodb = table.put_item(Item=pedido)
        pedidos.append(pedido)

        # Eliminar mensaje de la cola SQS
        receipt_handle = message['ReceiptHandle']
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
    
    # Respuesta final
    return {
        'statusCode': 200,
        'cantidad_pedidos_procesados': len(pedidos),  # Número de pedidos procesados
        'pedidos_procesados': pedidos  # Lista de pedidos procesados
    }
