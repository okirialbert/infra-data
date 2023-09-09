-- create dataset with metadata names
SELECT 
df_eco_op.action, df_eco_op.ref, df_eco_op.unit_measure, 
df_eco_op.obs_value, df_eco_op.measure, df_eco_op.time_period,
oecd_struct_meta.name AS unit_cat_name, o.name AS measure_cat_name, r.name AS ref_name
FROM df_eco_op
  INNER JOIN oecd_struct_meta ON df_eco_op.unit_measure=oecd_struct_meta.id AND df_eco_op.obs_value IS NOT NULL
  INNER JOIN oecd_struct_meta AS o ON df_eco_op.measure=o.id
  INNER JOIN oecd_struct_meta AS r ON df_eco_op.ref=r.id;
