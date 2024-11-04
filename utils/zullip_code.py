import os

import zulip
import markdown

client = zulip.Client(config_file=os.path.dirname(__file__)+"/zuliprc")

def send_msg_group(msg: str, to: int) -> any:
    request = {
        "type": "stream",
        "to": to,
        "topic": "channel events",
        "content": markdown.markdown(msg).replace("<p>", "").replace("</p>", "").replace("<pre><code>", "").replace(
            "</code></pre>", ""),
    }
    result = client.send_message(request)
    return result


def send_msg_private(msg: str, to: int) -> any:
    request = {
        "type": "private",
        "to": [to],
        "content": markdown.markdown(msg).replace("<p>", "").replace("</p>", "").replace("<pre><code>", "").replace(
            "</code></pre>", ""),
    }
    result = client.send_message(request)
    return result
