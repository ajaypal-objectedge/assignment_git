from main import engine
from sqlalchemy import  text
import logging as log 
import click 


log.basicConfig(filename='dbquerieslog.log',level=log.DEBUG,format='%(asctime)s - %(levelname)s - %(message)s')

def query_execution_function(query,is_select = False, **params) :
        log.debug(params)
        log.debug(f'query statement is : {query}')
        result = None
        try:
                conn = engine.connect()
                records = conn.execute(query,[params],)
                log.info("query executed successfully !")
                if is_select :
                        result = records.fetchall()
        except Exception as e :
                log.exception(f"operation not performed due to {e}")
        else : 
                if is_select == False :
                        conn.commit()
                        log.info('changes committed to database')
        finally:
                conn.close()
                return result

def update_allocated_qty_to_zero_python() :
        
        update_query  = text(""" UPDATE customer_partner_relation_table SET subscription_lines = (SELECT jsonb_agg(jsonb_set(element,'{allocated_qty}','0',false)) FROM jsonb_array_elements(subscription_lines) AS element);""")
        query_execution_function(update_query)
        log.info("Data updated")
            
#update_allocated_qty_to_zero_python()



def select_partner_customer_list(**dict):
        '''
        select_query = text(""" SELECT partner_sfdc_account_id as partner, ARRAY_AGG(customer_sfdc_account_id) as customers
	                        FROM customer_partner_relation_table 
                                GROUP BY partner_sfdc_account_id ; """)
        result = query_execution_function(select_query,True,**dict)
        log.info("data selected")
        return result
        '''

        stmt = "SELECT partner_sfdc_account_id as partner, ARRAY_AGG(customer_sfdc_account_id) as customers FROM customer_partner_relation_table WHERE 1=1 "
        if dict:
                if dict["customer"]:
                        stmt += f" AND customer_sfdc_account_id IN :customer "
                if dict["partner"]:
                        stmt += f"AND partner_sfdc_account_id IN :partner "
        stmt += "GROUP BY partner_sfdc_account_id "
        return query_execution_function(text(stmt),True,**dict)

def isPartnerHavingErrorStatus(partner) :
        
        partnerErrorStatusQuery = text("""SELECT partner_status FROM customer_partner_relation_table WHERE partner_sfdc_account_id = (:partnerid) AND partner_status = 'ERROR_STATUS'""")
        result_with_errorStatus_partner = query_execution_function(partnerErrorStatusQuery,True, partnerid = partner)
        
        if result_with_errorStatus_partner == [] :
                return False 
        else:
                return True  

def isCustomerHavingErrorStatus(customer):
       
        customerErrorStatusQuery = text("""SELECT customer_status FROM customer_partner_relation_table WHERE customer_sfdc_account_id = (:customerid) AND customer_status = 'ERROR_STATUS'""")
        result_with_errorStatus_customer = query_execution_function(customerErrorStatusQuery,True,customerid = customer)

        if result_with_errorStatus_customer != None :
                return False 
        else:
                return True       

def changePartnerStatus(partner,status) :

        query = text("UPDATE customer_partner_relation_table SET partner_status = (:status) WHERE partner_sfdc_account_id = (:partner)")
        query_execution_function(query,partner=partner,status=status)
        log.info(f"Changed partner_status to {status} for partner_sfdc_account_id : {partner}")

def changeCustomerStatus(partner,customer,status):

        query = text("UPDATE customer_partner_relation_table SET customer_status = (:status) WHERE partner_sfdc_account_id = :partner AND customer_sfdc_account_id = :customer")
        query_execution_function(query,customer=customer,partner=partner,status=status)
        log.info(f"Changed customer_status to {status} for partner_sfdc_account_id : {partner} and customer_sfdc_account_id : {customer}")

def setAllocatedQtyWithCalculations(customer,partner) :

        query = text(f"SELECT update_allocated_qty(:customer,:partner)")
        try :
                query_execution_function(query,False,partner=partner,customer=customer)
        except Exception as e :
                log.exception(f'could not perform updateallocatedqty function in sql due to {e}')
        else :
                log.info(f'Values added to subscription table for partner : {partner} , customer : {customer}')

def allCustomerReady(partner):
        query = text("SELECT 1 FROM customer_partner_relation_table WHERE partner_sfdc_account_id = (:partner) AND customer_status <> 'READY' ")
        result = query_execution_function(query,True,partner=partner)

        if result == None :
                return False 
        else :
                return True 

def process_data(**dict) :
        partner_customer_list = select_partner_customer_list(**dict)

        for each_partner_customer_list in partner_customer_list :
                partner = each_partner_customer_list[0]
                customers = each_partner_customer_list[1]

                # check partner error status :
                if isPartnerHavingErrorStatus(partner) :
                        log.info(f"partner_sfdc_account_id : {partner} is having ERROR_STATUS , so SKIPPING this partner")
                        continue
                else :
                        log.info(f"partner_sfdc_account_id : {partner} is NOT having ERROR_STATUS , going to loop over its customers ")
                        changePartnerStatus(partner , 'IN_PROGRESS')
                        for customer in customers :
                                if isCustomerHavingErrorStatus(customer) :
                                        log.info(f"customer_sfdc_account_id : {customer} is having ERROR_STATUS , so SKIPPING this customer")
                                        continue 
                                else :
                                        log.info(f"customer_sfdc_account_id : {customer} is NOT having ERROR_STATUS ")
                                        changeCustomerStatus(partner,customer,'IN_PROGRESS')
                                        try :
                                                setAllocatedQtyWithCalculations(customer,partner) 
                                        except Exception as e :
                                                changeCustomerStatus(customer,'ERROR_STATUS')
                                                log.info(f'got ERROR_STATUS FOR partner : {partner} and customer : {customer}')
                                                continue 
                                        else :
                                                changeCustomerStatus(partner,customer,'READY')
                        
                        
                        # if all customer status is ready mark partner status ready
                        if allCustomerReady(partner) :
                                log.info(f"All Customers are READY for partner : {partner}")
                                changePartnerStatus(partner,'READY')
                        else :
                                log.info(f"All Customers are NOT READY for partner : {partner}")
                break 


if __name__ == '__main__' :
        @click.command()
        @click.option('-p','--partner',multiple = True , help = 'partner_sfdc_account_id')
        @click.option('-c','--customer',multiple = True ,help='customer_sfdc_account_id')

        def runner(partner , customer):
             dict = {
            'partner' : partner , 
            'customer': customer
            }
             process_data(**dict)


runner() 