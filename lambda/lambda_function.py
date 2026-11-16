import json
import os
import urllib.parse
import boto3

s3 = boto3.client("s3")
sns = boto3.client("sns")


def lambda_handler(event, context):
    topic_arn = os.environ["SNS_TOPIC_ARN"]
    student_roll = os.environ.get("STUDENT_ROLL", "UNSET")

    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        print(f"New invoice detected: s3://{bucket}/{key}")

        invoice = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())

        subtotal = float(invoice["subtotal"])
        gst = round(subtotal * 0.18, 2)
        grand_total = round(subtotal + gst, 2)
        created_at = invoice.get("created_at", "N/A")

        summary = {
            "invoice_id": invoice["invoice_id"],
            "student_roll": student_roll,
            "customer_name": invoice["customer_name"],
            "item": invoice["item"],
            "subtotal": subtotal,
            "gst_18_percent": gst,
            "grand_total": grand_total,
            "created_at": created_at,
            "status": "PROCESSED",
        }

        summary_key = f"processed/{invoice['invoice_id']}_summary.json"
        s3.put_object(
            Bucket=bucket,
            Key=summary_key,
            Body=json.dumps(summary, indent=2),
            ContentType="application/json",
        )
        print(f"Summary written: s3://{bucket}/{summary_key}")

        subject = f"[{student_roll}] New invoice {invoice['invoice_id']} for {invoice['customer_name']}"
        body = (
            f"Invoice ID : {invoice['invoice_id']}\n"
            f"Customer   : {invoice['customer_name']}\n"
            f"Item       : {invoice['item']} (qty {invoice['quantity']})\n"
            f"Subtotal   : Rs {subtotal}\n"
            f"GST (18%)  : Rs {gst}\n"
            f"Grand Total: Rs {grand_total}\n"
            f"Summary    : s3://{bucket}/{summary_key}\n"
            f"Created at: {created_at}"
        )
        sns.publish(TopicArn=topic_arn, Subject=subject, Message=body)
        print("Email notification sent through SNS")

    return {"statusCode": 200, "body": json.dumps(summary)}