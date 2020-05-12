from datetime import datetime
from pprint import pprint

import boto3
import json

sessionProd = boto3.session.Session(region_name="us-east-1", profile_name='production')

resourceDynamodb = sessionProd.resource('dynamodb')
clientFirehose = sessionProd.client('firehose')
table = resourceDynamodb.Table('kushki-usrv-snr-primary-transactions')
table2 = resourceDynamodb.Table('usrv-snr-batch-primary-transaction')
STREAM = 'primary-kushki-usrv-snr-transaction-redshift'


def convert(n):
    return {
        'id': n['id'],
        'ticket_number': n['ticketNumber'],
        'credential': n['credential'],
        'document_number': n['documentNumber'],
        'document_type': n['documentType'],
        'email': n['email'],
        'kind': n['kind'],
        'office': n['office'],
        'registration': n['registration'],
        'pay_method': n['payMethod'],
        'transaction_id': n['transactionId'],
        'transaction_result': n['transactionResult'],
        'trazability_code': n['trazabilityCode'],
        'currency_code': 'COP',
        'iva_value': 1053.78,
        'subtotal_iva': 5546.22,
        'subtotal_iva0': 15900,
        'name': n['name'],
        'created': n['created'],
    }


if __name__ == "__main__":
    items = []
    response = table.scan(
        FilterExpression='transactionType = :a',
        ExpressionAttributeValues={
            ':a': 'batch'
        }
    )
    items += response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(
            FilterExpression='transactionType = :a',
            ExpressionAttributeValues={
                ':a': 'batch'
            },
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items += response['Items']

    print(len(items))

    for i in range(len(items)):
        items[i]['created'] = datetime.fromtimestamp(int(table2.get_item(
            Key={
                'id': items[i]['batchTransactionId']
            },
        )['Item']['created']) / 1000).strftime('%Y-%m-%d %H:%M:%S')

        print([{'Data': json.dumps(convert(items[i]))}])

        pprint(clientFirehose.put_record_batch(
            DeliveryStreamName=STREAM,
            Records=[{'Data': json.dumps(convert(items[i]))}]
        ))
