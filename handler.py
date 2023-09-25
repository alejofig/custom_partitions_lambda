import base64
import json
import copy


def create_partitions(message):
    partition_keys = {
        "partition1": message["partition1"],
        "partition2": message["partition2"]
    }
    return partition_keys


def lambda_handler(event, context):
    output = []


    for record in event['records']:
        payload = base64.b64decode(record['data'])
        json_string = payload.decode("utf-8")
        message = json.loads(json_string)
        partitions = create_partitions(message)
        final_message = json.dumps(message)
        output_json_with_line_break = final_message + "\n"
        encoded_bytes = base64.b64encode(bytearray(output_json_with_line_break, 'utf-8'))
        encoded_string = str(encoded_bytes, 'utf-8')
        output_record = copy.deepcopy(record)
        output_record['data'] = encoded_string
        output_record['result'] = 'Ok'
        output_record['metadata'] = {'partitionKeys': partitions}
        output.append(output_record)
    return {'records': output}
