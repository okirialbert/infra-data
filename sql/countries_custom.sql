SUM(CASE WHEN ref IN 
('ARG','BOL','BRA','CHL','COL','ECU','FLK',
'GUF','GUY','PRY','PER','SUR','URY','VEN')
THEN obs_value ELSE 0 END)

SUM(CASE WHEN ref IN 
('AIA','ATG','ABW','BRB','BLZ','BMU',
'BES','VGB','CAN','CYM','CRI','CUB',
'CUW','DMA','DOM','SLV','GRL','GRD',
'GLP','GTM','HTI','HND','JAM','MTQ',
'MEX','MSR','ANT','NIC','PAN','PRI',
'BLM','KNA','LCA','MAF','SPM','VCT','SXM',
'BHS','TTO','TCA','USA','VIR')THEN obs_value ELSE 0 END)

SUM(CASE WHEN ref IN 
('AFG','ARM','AZE','BHR','BGD',
'BTN','IOT','BRN','KHM','CHN','CCK',
'GEO','HKG','IND','IDN','IRN','IRQ','ISR',
'JPN','JOR','KAZ','KWT','KGZ','LAO','LBN',
'MAC','MYS','MDV','MNG','MMR','NPL','PRK','OMN',
'PAK','PSE','PHL','QAT','SAU','SGP','KOR',
'LKA','SYR','TWN','TJK','THA','TUR','TKM','ARE',
'UZB','VNM','YEM')THEN obs_value ELSE 0 END)

SUM(CASE WHEN ref IN 
('AUT','BEL','BGR','HRV','CYP','CZE',
'DNK','EST','FIN','FRA','DEU','GRC','HUN',
'IRL','ITA','LVA','LTU','LUX','MLT','NLD','POL','PRT',
'ROU','SVK','SVN','ESP','SWE','ALB','AND','ARM',
'BLR','BIH','FRO','GEO','GIB','ISL','IMN',
'XKX','LIE','MKD','MDA','MCO','MNE','NOR','RUS',
'SMR','SRB','CHE','TUR','UKR','GBR','VAT'
)THEN obs_value ELSE 0 END)

SUM(CASE WHEN ref IN 
('DZA','AGO','BEN','BWA','BFA','BDI','CMR','CPV',
'CAF','TCD','COM','COG','COD','CIV','DJI','EGY','GNQ',
'ERI','ETH','GAB','GMB','GHA','GIN','GNB','KEN','LSO',
'LBR','LBY','MDG','MLI','MWI','MRT','MUS','MYT','MAR',
'MOZ','NAM','NER','NGA','REU','RWA','STP','SEN','SYC','SLE',
'SOM','ZAF','SSD','SDN','SWZ','TZA','TGO','TUN','UGA','ESH'
)THEN obs_value ELSE 0 END)