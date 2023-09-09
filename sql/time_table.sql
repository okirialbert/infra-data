-- create dataset with concatenated unit and measure names
SELECT 
df_eco_op.ref, df_eco_op.unit_measure, 
df_eco_op.obs_value, df_eco_op.measure, df_eco_op.time_period,
o.name || ' Unit: ' || oecd_struct_meta.name AS meas_unit_name, r.name AS ref_name
FROM df_eco_op
  INNER JOIN oecd_struct_meta ON df_eco_op.unit_measure=oecd_struct_meta.id AND df_eco_op.obs_value IS NOT NULL
  INNER JOIN oecd_struct_meta AS o ON df_eco_op.measure=o.id
  INNER JOIN oecd_struct_meta AS r ON df_eco_op.ref=r.id;