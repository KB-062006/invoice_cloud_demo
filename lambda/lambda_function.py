import json
import os
from urllib.parse import unquote_plus
import boto3

s3 = boto3.client("s3")
sns = boto3.client("sns")


def lambda_handler(event, context):
    topic_arn = os.environ["SNS_TOPIC_ARN"]
    student_roll = os.environ.get("STUDENT_ROLL", "UNSET")

    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        print(f"New invoice detected: s3://{bucket}/{key}")

        # 1. Read the raw invoice JSON from S3
        invoice = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())

        # 2. Extract fields safely regardless of key naming
        invoice_id = invoice.get("invoice_id") or invoice.get("id") or key.split("/")[-1].replace(".json", "")
        customer = invoice.get("customer") or invoice.get("customer_name") or "Unknown"
        item = invoice.get("item") or "Item"
        quantity = int(invoice.get("quantity", 1))
        created_at = invoice.get("created_at", "N/A")

        if "subtotal" in invoice:
            subtotal = float(invoice["subtotal"])
        elif "unit_price" in invoice:
            subtotal = round(quantity * float(invoice["unit_price"]), 2)
        elif "price" in invoice:
            subtotal = round(quantity * float(invoice["price"]), 2)
        else:
            subtotal = float(invoice.get("amount", 0))

        gst = round(subtotal * 0.18, 2)
        grand_total = round(subtotal + gst, 2)

        summary = {
            "invoice_id": invoice_id,
            "student_roll": student_roll,
            "customer": customer,
            "item": item,
            "quantity": quantity,
            "subtotal": subtotal,
            "gst_18_percent": gst,
            "grand_total": grand_total,
            "created_at": created_at,
            "status": "PROCESSED",
        }

        # 3. Write summary JSON under processed/
        summary_key = f"processed/{invoice_id}_summary.json"
        s3.put_object(
            Bucket=bucket,
            Key=summary_key,
            Body=json.dumps(summary, indent=2),
            ContentType="application/json",
        )
        print(f"Summary written: s3://{bucket}/{summary_key}")

        # 4. Publish email alert via SNS
        subject = f"[{student_roll}] New invoice {invoice_id} for {customer}"
        body = (
            f"Invoice ID : {invoice_id}\n"
            f"Customer   : {customer}\n"
            f"Item       : {item} (qty {quantity})\n"
            f"Subtotal   : Rs {subtotal}\n"
            f"GST (18%)  : Rs {gst}\n"
            f"Grand Total: Rs {grand_total}\n"
            f"Summary    : s3://{bucket}/{summary_key}\n"
            f"Created at: {created_at}"
        )
        sns.publish(TopicArn=topic_arn, Subject=subject, Message=body)
        print("Email notification sent through SNS")

    return {"statusCode": 200, "body": json.dumps(summary)}