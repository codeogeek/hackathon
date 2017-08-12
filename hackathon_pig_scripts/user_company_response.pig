inp_data = LOAD 'Customer_complaints.csv' USING PigStorage(',') AS (date_received:CHARARRAY,product:CHARARRAY,sub_product:CHARARRAY,issue:CHARARRAY,sub_issue:CHARARRAY,consumer_complaint_narrative:CHARARRAY,company_public_response:CHARARRAY,company:CHARARRAY,state:CHARARRAY,zip_code:CHARARRAY,tags:CHARARRAY,consumer_consent_provided:CHARARRAY,submitted_via:CHARARRAY,date_sent_to_company:CHARARRAY,company_response_to_consumer:CHARARRAY,timely_response:CHARARRAY,consumer_disputed:CHARARRAY,complaint_id:INT);

filter_data = FILTER inp_data 
		BY (company != '') AND (company_response_to_consumer != '') AND (timely_response != '');
grp_by_product_issue_sub_issue = GROUP filter_data 
					BY (company,company_response_to_consumer,timely_response);
gen_0 = FOREACH grp_by_product_issue_sub_issue
			GENERATE
				FLATTEN(group),
				COUNT(filter_data.company) AS issue_cnt;

STORE gen_0 INTO '/home/cloudera/Desktop/company_response' USING PigStorage(',');

