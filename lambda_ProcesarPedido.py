import json
import boto3

def lambda_handler(event, context):

    # Create SQS client
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/498917627164/sqs-pedidos'
    
    # Receive message from SQS queue
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
        pedidos.append(pedido)
        response_dynamodb = table.put_item(Item=pedido)
        receipt_handle = message['ReceiptHandle']
        # Delete received message from queue
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )

    return {
        'statusCode': 200,
        'pedidos_procesados': pedidos
    }
    