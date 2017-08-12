inp_data = LOAD 'Customer_complaints.csv' USING PigStorage(',') AS (date_received:CHARARRAY,product:CHARARRAY,sub_product:CHARARRAY,issue:CHARARRAY,sub_issue:CHARARRAY,consumer_complaint_narrative:CHARARRAY,company_public_response:CHARARRAY,company:CHARARRAY,state:CHARARRAY,zip_code:CHARARRAY,tags:CHARARRAY,consumer_consent_provided:CHARARRAY,submitted_via:CHARARRAY,date_sent_to_company:CHARARRAY,company_response_to_consumer:CHARARRAY,timely_response:CHARARRAY,consumer_disputed:CHARARRAY,complaint_id:INT);

filter_lack_of_information = FILTER inp_data BY (((issue matches '.*information.*') OR (issue matches '.*Information.*')) AND ((issue matches '.*Incorrect.*') OR (issue matches '.*not.*') OR (issue matches '.*Didn.*') )) OR (((sub_issue matches '.*Information.*') OR ((sub_issue matches '.*Information.*'))) AND ((sub_issue matches '.*Incorrect.*') OR (sub_issue matches '.*not.*') OR (sub_issue matches '.*Didn.*')));

gen_0 = FOREACH filter_lack_of_information GENERATE 'lack_of_information' as problem, state as location;

filter_purchase = FILTER inp_data BY ((issue matches '.*purchase.*') OR (sub_issue matches '.*purchase.*')); 

gen_1 = FOREACH filter_purchase GENERATE 'problem_in_purchase' as problem, state as location;

filter_threaten = FILTER inp_data BY ((issue matches '.*threatened.*') OR (issue matches '.*Threatened.*')) OR ((sub_issue matches '.*threatened.*') OR (sub_issue matches '.*Threatened.*'));

gen_2 = FOREACH filter_threaten GENERATE 'threatened' as problem, state as location;

filter_debt = FILTER inp_data BY (((issue matches '.*debt.*') OR (issue matches '.*Debt.*')) AND (NOT(issue matches '.*information.*') OR NOT(issue matches '.*Information.*')) ) OR (((sub_issue matches '.*debt.*') OR (sub_issue matches '.*Debt.*')) AND ((sub_issue matches '.*information.*') OR (sub_issue matches '.*Information.*')));

gen_3 = FOREACH filter_debt GENERATE 'problem_in_debt' as problem, state as location;

filter_struggle = FILTER inp_data BY (((issue matches '.*struggl.*') OR (issue matches '.*Struggl.*')) AND (NOT(issue matches '.*struggl.*') OR NOT(issue matches '.*Struggl.*')));

gen_4 = FOREACH filter_struggle GENERATE 'struggling_to_pay' as problem, state as location;

full_union = UNION gen_0, gen_1, gen_2, gen_3, gen_4;

group_full_join = GROUP full_union BY problem,location;

gen_final = FOREACH group_full_join GENERATE FLATTEN(group), location as location, count(full_union.location);

STORE gen_0 INTO '/home/cloudera/Desktop/reason_problem.csv' USING PigStorage(',');

