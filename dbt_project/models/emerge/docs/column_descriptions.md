{% docs emerge_person_ex_release_20260123_emerge_id %}
Unique de-identified eMERGE ID formatted as SITE_ID + 5 digit randomized Ex: 2713331; 7 total digits
{% enddocs %}

{% docs emerge_person_ex_release_20260123_withdrawal_status %}
Participant's withdrawal status
{% enddocs %}

{% docs emerge_person_ex_release_20260123_year_of_birth %}
Participant year of birth for age calcuation and quality control purposes
{% enddocs %}

{% docs emerge_person_ex_release_20260123_gender_concept_id %}
This field is meant to capture the biological sex at birth of the Person. This field should not be used to study gender identity issues.
{% enddocs %}

{% docs emerge_person_ex_release_20260123_race_concept_id %}
This field captures race or ethnic background of the person.
{% enddocs %}

{% docs emerge_person_ex_release_20260123_ethnicity_concept_id %}
This field captures Ethnicity as defined by the Office of Management and Budget (OMB) of the US Government: it distinguishes only between â€œHispanicâ€ and â€œNot Hispanicâ€. Races and ethnic backgrounds are not stored here.
{% enddocs %}

{% docs emerge_measurement_ex_release_20260127_emerge_id %}
Unique de-identified eMERGE ID formatted as SITE_ID + 5 digit randomized Ex: 2713331; 7 total digits
{% enddocs %}

{% docs emerge_measurement_ex_release_20260127_measurement__5f480161 %}
This is the standard concept mapped from the source value which represents a measurement
{% enddocs %}

{% docs emerge_measurement_ex_release_20260127_value_as_number %}
This is the numerical value of the result of the measurement
{% enddocs %}

{% docs emerge_measurement_ex_release_20260127_value_as_text %}
This is the text value of the result of the measurement
{% enddocs %}

{% docs emerge_measurement_ex_release_20260127_range_low %}
Lower limits of the normal range for a measurement result, These ranges are assumed to be in the same units of measure as the measurement value.
{% enddocs %}

{% docs emerge_measurement_ex_release_20260127_range_high %}
Upper limits of the normal range for a measurement result, These ranges are assumed to be in the same units of measure as the measurement value.
{% enddocs %}

{% docs emerge_measurement_ex_release_20260127_range_flag %}
Flag indicating a lab result is lower or higher than a normal value
{% enddocs %}

{% docs emerge_measurement_ex_release_20260127_unit_concept_id %}
There is currently no vocabulary link between measurement Concepts and unit Concepts, the UNIT_CONCEPT_ID is a representation of the units as provided by the source data after ETL clean up.
{% enddocs %}

{% docs emerge_measurement_ex_release_20260127_unit_concept_as_text %}
Measurement unit text value
{% enddocs %}

{% docs emerge_measurement_ex_release_20260127_row_id %}
Unique number differentiating identical rows to avoid duplicate rows in data file
{% enddocs %}

{% docs emerge_measurement_ex_release_20260127_encounter_id %}
Indicates a sequence of events for same age encounters (events that occur on the same day for an eMERGE ID have the same encounter number)
{% enddocs %}

{% docs emerge_measurement_ex_release_20260127_gira_ror %}
A flag indicating if an encounter occurred before or after participant GIRA return
{% enddocs %}

{% docs emerge_bmi_ex_release_20260128_emerge_id %}
Unique de-identified eMERGE ID formatted as SITE_ID + 5 digit randomized
 Ex: 2713331; 7 total digits
{% enddocs %}

{% docs emerge_bmi_ex_release_20260128_age_at_event %}
Age of participant at time of BMI measurement occurrence
{% enddocs %}

{% docs emerge_bmi_ex_release_20260128_measurement_concept_id %}
This is the standard concept mapped from the source value which represents a measurement
{% enddocs %}

{% docs emerge_bmi_ex_release_20260128_measurement_concept_name %}
Name of measurement mapped from MEASUREMENT_CONCEPT_ID
{% enddocs %}

{% docs emerge_bmi_ex_release_20260128_value_as_number %}
This is the numerical value of the result of the measurement
{% enddocs %}

{% docs emerge_bmi_ex_release_20260128_unit_concept_id %}
Value should be linked to a standard concept within the unit domain that most accurately reflects the unit provided in the source data.
{% enddocs %}

{% docs emerge_bmi_ex_release_20260128_unit_concept_name %}
Name of unit used to express the value of a measurement or observation
{% enddocs %}

{% docs emerge_bmi_ex_release_20260128_bmi_z_score %}
Gender adjusted Z-Score and age adjusted for pediatrics
{% enddocs %}

{% docs emerge_bmi_ex_release_20260128_row_id %}
Unique number differentiating identical rows to avoid duplicate rows in data file
{% enddocs %}

{% docs emerge_bmi_ex_release_20260128_encounter_id %}
Indicates a sequence of events for same age encounters (events that occur on the same day for an eMERGE ID have the same encounter number)
{% enddocs %}

{% docs emerge_bmi_ex_release_20260128_gira_ror %}
A flag indicating if an encounter occurred before or after participant GIRA return
{% enddocs %}

{% docs emerge_cpt_ex_release_20260129_emerge_id %}
Unique de-identified eMERGE ID formatted as SITE_ID + 5 digit randomized
 Ex: 2713331; 7 total digits
{% enddocs %}

{% docs emerge_cpt_ex_release_20260129_age_at_event %}
Age of participant at time of CPT occurrence
{% enddocs %}

{% docs emerge_cpt_ex_release_20260129_cpt_code %}
Indicates the originating CPT code
{% enddocs %}

{% docs emerge_cpt_ex_release_20260129_row_id %}
Unique number differentiating identical rows to avoid duplicate rows in data file
{% enddocs %}

{% docs emerge_cpt_ex_release_20260129_encounter_id %}
Indicates a sequence of events for same age encounters (events that occur on the same day for an eMERGE ID have the same encounter number)
{% enddocs %}

{% docs emerge_cpt_ex_release_20260129_gira_ror %}
A flag indicating if an encounter occurred before or after participant GIRA return
{% enddocs %}

{% docs emerge_cpt_ex_release_20260129_none %}
None
{% enddocs %}

{% docs emerge_cpt_ex_release_20260129_redacted_codes_list %}
Redacted Codes List
{% enddocs %}

{% docs emerge_icd_ex_release_20260129_emerge_id %}
Unique de-identified eMERGE ID formatted as SITE_ID + 5 digit randomized
 Ex: 2713331; 7 total digits
{% enddocs %}

{% docs emerge_icd_ex_release_20260129_age_at_event %}
Age of participant at time of ICD occurrence
{% enddocs %}

{% docs emerge_icd_ex_release_20260129_icd_code %}
Indicates the originating ICD code
{% enddocs %}

{% docs emerge_icd_ex_release_20260129_icd_flag %}
Numeric flag, should indicate if code is a 9 or 10
{% enddocs %}

{% docs emerge_icd_ex_release_20260129_row_id %}
Unique number differentiating identical rows to avoid duplicate rows in data file
{% enddocs %}

{% docs emerge_icd_ex_release_20260129_encounter_id %}
Indicates a sequence of events for same age encounters (events that occur on the same day for an eMERGE ID have the same encounter number)
{% enddocs %}

{% docs emerge_icd_ex_release_20260129_gira_ror %}
A flag indicating if an encounter occurred before or after participant GIRA return
{% enddocs %}