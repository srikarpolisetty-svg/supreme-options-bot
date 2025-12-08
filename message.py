import time
from datetime import datetime
import smtplib
from email.message import EmailMessage
from trading_algo_SPY import trading_signal

def send_text(message: str):
    msg = EmailMessage()
    msg.set_content(message)
    msg["From"] = "srikarpolisetty@gmail.com"
    msg["To"] = "srikarpolisetty@gmail.com"
    msg["Subject"] = "Trading Alert"

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login("srikarpolisetty@gmail.com", "qxbd zauk zbbz aoqp")
            smtp.send_message(msg)
    except Exception as e:
        print("ERROR in send_text:", repr(e))


last_signal = None  # store last signal sent


while True:
    signal = trading_signal()
    now = datetime.utcnow()

    # Only send if the signal CHANGED
    if signal != last_signal:
        if signal == "buy it":
            send_text("BUY SIGNAL: SPY")

        elif signal == "sell current price is less than the 50 day moving average !!!":
            send_text("SELL SIGNAL: SPY")

        elif signal == "hold it":
            send_text("HOLD SIGNAL: SPY")

        last_signal = signal  # update the last signal

    # Print so you can see what it's doing
    print(f"{now} | SIGNAL: {signal}")

    # Wait 10 minutes
    time.sleep(600)


 



