-- Delete redundant rows

DELETE FROM test_table R1
    USING test_table R2
    WHERE R2.obs_id < R1.obs_id 
    AND R1.ref = R2.ref 
    AND R1.measure = R2.measure 
    AND R1.unit_measure = R2.unit_measure 
    AND R1.time_period = R2.time_period 
    AND R1.obs_value = R2.obs_value 
    AND R1.structure_id = R2.structure_id;