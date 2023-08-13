CREATE OR REPLACE MODEL `prediction_model.daily_footfall_predictor`
OPTIONS(model_type='RANDOM_FOREST_REGRESSOR',
input_label_cols=['total_footfall']) AS
SELECT * FROM `prediction_model.input_data_daily`
