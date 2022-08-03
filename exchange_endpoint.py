from flask import Flask, request, g
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from flask import jsonify
import json
import eth_account
import algosdk
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import load_only
from datetime import datetime
import sys

from models import Base, Order, Log
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)

app = Flask(__name__)

@app.before_request
def create_session():
    g.session = scoped_session(DBSession)

@app.teardown_appcontext
def shutdown_session(response_or_exc):
    sys.stdout.flush()
    g.session.commit()
    g.session.remove()


""" Suggested helper methods """
def verify(content):

    try:

        if content['payload']['platform'] == 'Ethereum':
            eth_sk = content['sig']
            eth_pk = content['payload']['sender_pk']

            payload = json.dumps(content['payload'])
            eth_encoded_msg = eth_account.messages.encode_defunct(text=payload)
            recovered_pk = eth_account.Account.recover_message(eth_encoded_msg, signature=eth_sk)

            # Check if signature is valid
            if recovered_pk == eth_pk:
                result = True
            else:
                result = False

            return result           # bool value

        if content['payload']['platform'] == 'Algorand':
            algo_sig = content['sig']
            algo_pk = content['payload']['sender_pk']
            payload = json.dumps(content['payload'])
            
            result = algosdk.util.verify_bytes(payload.encode('utf-8'), algo_sig, algo_pk)
            return result           # bool value 

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        print(e)

        
def process_order(order):

    #1. Insert new order
    order_obj = Order(    sender_pk=order['sender_pk'],
                          receiver_pk=order['receiver_pk'], 
                          buy_currency=order['buy_currency'], 
                          sell_currency=order['sell_currency'], 
                          buy_amount=order['buy_amount'], 
                          sell_amount=order['sell_amount'], 
                          exchange_rate=(order['buy_amount']/order['sell_amount'])
                      )
    

    g.session.add(order_obj)
    g.session.commit()

    # check up if it works well and get the order id
    results = g.session.execute("select distinct id from orders where " + 
                            " sender_pk = '" + str(order['sender_pk']) + "'" +
                            " and receiver_pk = '" + str(order['receiver_pk']) + "'")

    order_id = results.first()['id']
    # print(" new order: ", order_id, order['buy_currency'], order['sell_currency'], order['buy_amount'], order['sell_amount'])

    #2. Matching order
    results = g.session.execute("select count(id) " + 
                            " from orders where orders.filled is null " + 
                            " and orders.sell_currency = '" + order['buy_currency'] + "'" +
                            " and orders.buy_currency = '" + order['sell_currency'] + "'" +
                            " and exchange_rate <= " + str(order['sell_amount']/order['buy_amount']))

    if results.first()[0] == 0:
        # print("::::no matching order::::")
        return

    results = g.session.execute("select distinct id, sender_pk, receiver_pk, buy_currency, sell_currency, buy_amount, sell_amount " + 
                            "from orders where orders.filled is null " + 
                            " and orders.sell_currency = '" + order['buy_currency'] + "'" +
                            " and orders.buy_currency = '" + order['sell_currency'] + "'" +
                            " and exchange_rate <= " + str(order['sell_amount']/order['buy_amount'])) 

    for row in results:
        m_order_id = row['id']
        m_sender_pk = row['sender_pk']
        m_receiver_pk = row['receiver_pk'] 
        m_buy_currency = row['buy_currency'] 
        m_sell_currency = row['sell_currency'] 
        m_buy_amount = row['buy_amount']
        m_sell_amount = row['sell_amount']
        # print(" matched at ID: ", m_order_id)
        break

    # print(" matching order: ", m_order_id, m_buy_currency, m_sell_currency, m_buy_amount, m_sell_amount)
    # print(" order['sell_amount']/order['buy_amount']: ", order['sell_amount']/order['buy_amount'], ">=", "(buy_amount/sell_amount)", (m_buy_amount/m_sell_amount))

    # update both the matching orders 
    stmt = text("UPDATE orders SET counterparty_id=:id, filled=:curr_date WHERE id=:the_id")
    stmt = stmt.bindparams(the_id=order_id, id=m_order_id, curr_date=datetime.now())
    g.session.execute(stmt)  # where session has already been defined

    stmt = text("UPDATE orders SET counterparty_id=:id, filled=:curr_date WHERE id=:the_id")
    stmt = stmt.bindparams(the_id=m_order_id, id=order_id, curr_date=datetime.now())
    g.session.execute(stmt)  # where session has already been defined
  
    #3. Create derived order
    if order['buy_amount'] > m_sell_amount:
        order_obj = Order(  sender_pk=order['sender_pk'],
                            receiver_pk=order['receiver_pk'], 
                            buy_currency=order['buy_currency'], 
                            sell_currency=order['sell_currency'], 
                            buy_amount=order['buy_amount'] - m_sell_amount, 
                            sell_amount=order['sell_amount'] - ((order['sell_amount']/order['buy_amount']) * m_sell_amount),
                            exchange_rate = (order['buy_amount'] - m_sell_amount)/(order['sell_amount'] - ((order['sell_amount']/order['buy_amount']) * m_sell_amount)),
                            creator_id=order_id)
        g.session.add(order_obj)
        g.session.commit()

    elif order['buy_amount'] < m_sell_amount:
        order_obj = Order(  sender_pk=m_sender_pk,
                            receiver_pk=m_receiver_pk, 
                            buy_currency=m_buy_currency, 
                            sell_currency=m_sell_currency, 
                            buy_amount= m_buy_amount - (m_buy_amount/m_sell_amount) * order['buy_amount'], 
                            sell_amount= m_sell_amount - order['buy_amount'],
                            exchange_rate = (m_buy_amount - (m_buy_amount/m_sell_amount) * order['buy_amount'])/(m_sell_amount - order['buy_amount']),
                            creator_id=m_order_id)
        g.session.add(order_obj)
        g.session.commit()


def log_message(d):
    # Takes input dictionary d and writes it to the Log table

    payload = json.dumps(d['payload'])

    try:
        # Insert new log
        log_obj = Log(message = json.dumps(d['payload']))

        g.session.add(log_obj)
        g.session.commit()

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        print(e)
""" End of helper methods """



@app.route('/trade', methods=['POST'])
def trade():
    
    try:

        print("In trade endpoint")
        if request.method == "POST":
            content = request.get_json(silent=True)
            print( f"content = {json.dumps(content)}" )
            columns = [ "sender_pk", "receiver_pk", "buy_currency", "sell_currency", "buy_amount", "sell_amount", "platform" ]
            fields = [ "sig", "payload" ]

            for field in fields:
                if not field in content.keys():
                    print( f"{field} not received by Trade" )
                    print( json.dumps(content) )
                    log_message(content)
                    return jsonify( False )

            for column in columns:
                if not column in content['payload'].keys():
                    print( f"{column} not received by Trade" )
                    print( json.dumps(content) )
                    log_message(content)
                    return jsonify( False )

            #Your code here

            #Note that you can access the database session using g.session
            if verify(content) is True: 

                order_obj = Order(sender_pk=content['payload']['sender_pk'],
                                  receiver_pk=content['payload']['receiver_pk'], 
                                  buy_currency=content['payload']['buy_currency'], 
                                  sell_currency=content['payload']['sell_currency'], 
                                  buy_amount=content['payload']['buy_amount'], 
                                  sell_amount=content['payload']['sell_amount'], 
                                  exchange_rate=(content['payload']['buy_amount']/content['payload']['sell_amount']),
                                  signature=content['sig'])

                process_order(order_obj)
                
            else:
                log_message(content)

            return jsonify( True )
        
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        print(e)
        

@app.route('/order_book')
def order_book():
    try:
        results = g.session.execute("select sender_pk, receiver_pk, buy_currency, sell_currency, buy_amount, sell_amount, signature " + 
                            "from orders ")

        result_list = list()
        for row in results:
            item = dict()
            item['sender_pk'] = row['sender_pk']
            item['receiver_pk'] = row['receiver_pk']
            item['buy_currency'] = row['buy_currency']
            item['sell_currency'] = row['sell_currency']
            item['buy_amount'] = row['buy_amount']
            item['sell_amount'] = row['sell_amount']
            item['signature'] = row['signature']

            result_list.append(item)

        result = dict()
        result['data'] = result_list

        return jsonify(result)

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        print(e)
        

if __name__ == '__main__':
    app.run(port='5002')
