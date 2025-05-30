import os

import zulip
import markdown

client = zulip.Client(config_file=os.path.dirname(__file__)+"/zuliprc")

def send_msg_group(msg: str, to: int) -> any:
    if "/todo" in msg:
        msg = markdown.markdown(msg).replace("<p>", "").replace("</p>", "").replace("<pre><code>", "").replace("</code></pre>", "")

    request = {
        "type": "stream",
        "to": to,
        "topic": "channel events",
        "content": msg,
    }
    result = client.send_message(request)
    return result


def send_msg_private(msg: str, to: int) -> any:
    if "/todo" in msg:
        msg = markdown.markdown(msg).replace("<p>", "").replace("</p>", "").replace("<pre><code>", "").replace("</code></pre>", "")

    request = {
        "type": "private",
        "to": [to],
        "content": msg,
    }
    result = client.send_message(request)
    return result

def upload_file_group(file_path: str, to: int, msg: str = '', topic: str = '') -> any:
    with open(file_path, 'rb') as f:
        result = client.upload_file(f)
        upload_url = result["url"]
        file_name = upload_url.split("/")[-1]
        request = {
            "type": "stream",
            "to": to,
            "topic": topic,
            "content": msg.strip() + "\n" + "[{}]({})".format(file_name,result["url"])
        }
        result = client.send_message(request)
        return result

def upload_file_private(file_path: str, to: int, msg: str = '') -> any:
    with open(file_path, 'rb') as f:
        result = client.upload_file(f)
        upload_url = result["url"]
        file_name = upload_url.split("/")[-1]
        request = {
            "type": "private",
            "to": [to],
            "content": msg.strip() + "\n" + "[{}]({})".format(file_name,result["url"])
        }
        result = client.send_message(request)
        return result