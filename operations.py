from main import engine
from sqlalchemy import  text
import logging as log 

log.basicConfig(filename='dbquerieslog.log',level=log.DEBUG,format='%(asctime)s - %(levelname)s - %(message)s')

def query_execution_function(query,is_select = False,**params) :
        log.debug(params)
        log.debug(f'query statement is : {query}')
        try:
                conn = engine.connect()
                records = conn.execute(query,[params],)
                log.info("query executed successfully !")
                if is_select :
                        result = records.fetchall()
        except Exception as e :
                log.exception(f"operation not performed due to {e}")
        else : 
                conn.commit()
                log.info('changes committed to database')
        finally:
                conn.close()
                return result

def update_allocated_qty_to_zero_python() :
        
        update_query  = text(""" UPDATE onebatch SET subscription_lines = (SELECT jsonb_agg(jsonb_set(element,'{allocated_qty}','0',false)) FROM jsonb_array_elements(subscription_lines) AS element);""")
        query_execution_function(update_query)
        log.info("Data updated")
            
#update_allocated_qty_to_zero_python()

def select_partner_customer_list():

        select_query = text(""" SELECT partner_sfdc_account_id as partner, ARRAY_AGG(customer_sfdc_account_id) as customers
	                        FROM onebatch 
                                GROUP BY partner_sfdc_account_id ; """)
        result = query_execution_function(select_query,True)
        log.info("data selected")
        return result


def checkErrorStatus(partner,customer):
        
        # check partner status 
        partnerErrorStatusQuery = text("""SELECT partner_status FROM onebatch WHERE partner_status = ERROR_STATUS""")
        result_with_errorStatus_partner = query_execution_function(partnerErrorStatusQuery)
        
        # check customer status
        customerErrorStatusQuery = text("""SELECT customer_status FROM onebatch WHERE customer_status = ERROR_STATUS""")
        result_with_errorStatus_customer = query_execution_function(customerErrorStatusQuery)

        if result_with_errorStatus_partner != None or result_with_errorStatus_customer != None :
                return True 
        else:
                return False       


partner_customer_list = select_partner_customer_list()

for each_partner_customer_list in partner_customer_list :
        partner = each_partner_customer_list[0]
        customers = each_partner_customer_list[1]
        for customer in customers :
                if checkErrorStatus(partner,customer) == False :
                        log.info(f"NO ERROR STATUS for partner_sfdc_account_id : {partner}")
                        #change_status_to_INPROGRESS(partner,customer)
                






         


        