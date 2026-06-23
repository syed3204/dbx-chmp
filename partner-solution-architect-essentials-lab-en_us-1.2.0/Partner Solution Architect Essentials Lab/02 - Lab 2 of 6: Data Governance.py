# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2 of 6: Data Governance

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Explore the Notebooks
# MAGIC
# MAGIC 1. **Open** the notebook **02.1-UC-data-governance-security-churn** to explore the concepts and practical usage of **Unity Catalog** for access control.
# MAGIC 2. **Read the Instructions** to understand Unity Catalog access control.
# MAGIC
# MAGIC Once you have reviewed the Unity Catalog exercises, proceed to the next section.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Enhance Unity Catalog Security and Governance
# MAGIC
# MAGIC Overview: Extend existing Unity Catalog capabilities by creating a more advanced masking function and applying row-level filters based on multiple attributes.
# MAGIC
# MAGIC <div style="border-left: 4px solid #ff9800; background: #fff3e0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">Tip</strong>
# MAGIC <div style="color:#333;">Use <strong>Genie Code</strong> to help write the masking function and row filter. Make changes one step at a time and validate after each change before moving to the next.</div>
# MAGIC </div>
# MAGIC
# MAGIC ### B1. Create an Advanced Masking Function
# MAGIC
# MAGIC **Actions:**
# MAGIC
# MAGIC **Enhance the existing `simple_mask` function** to perform more dynamic and conditional masking. Your updated function should:
# MAGIC - Adjust the masking of the PII data to be for all users **except** those belonging to the `hls_admin` **AND** `retail_admin` groups.
# MAGIC - Implement **partial masking** where only part of the sensitive data is visible (e.g., showing the first 3 characters of an email address and masking the rest).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. Verification: Paste Your Code
# MAGIC
# MAGIC <div style="border-left: 4px solid #4caf50; background: #e8f5e9; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#2e7d32; margin-bottom:6px; font-size: 1.1em;">Proof of Execution</strong>
# MAGIC <div style="color:#333;">After completing the tasks, paste your updated code in the cell below for review. This cell is for documentation only -- the code does not need to run here.</div>
# MAGIC </div>

# COMMAND ----------

# Paste your updated masking function and row filter code here for review (do not run this cell):

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B3. Verification: Screenshot of Results
# MAGIC
# MAGIC <div style="border-left: 4px solid #4caf50; background: #e8f5e9; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#2e7d32; margin-bottom:6px; font-size: 1.1em;">Proof of Execution</strong>
# MAGIC <div style="color:#333;">Provide evidence that the updated masking and row filtering logic is working correctly by saving a screenshot of the resulting table.</div>
# MAGIC </div>
# MAGIC
# MAGIC 1. Run the `%run` cell below to load the evidence functions.
# MAGIC 2. Run the `upload_evidence` code cell. When the widget appears, click the **Upload Screenshot** button in the upload widget below, navigate to your saved screenshot, and select **Open**.
# MAGIC 3. Run the `show_evidence` code cell to verify the image appears correctly.

# COMMAND ----------

# MAGIC %run ./Includes/Images/Image_Display_Function

# COMMAND ----------

upload_evidence("evidence-02-masking.png")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #ff9800; background: #fff3e0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">Note</strong>
# MAGIC <div style="color:#333;">If the upload widget does not appear after 30 seconds, re-run the cell above. Repeat until the widget appears.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4caf50; background: #e8f5e9; padding: 10px 14px; border-radius: 4px; margin: 12px 0;">
# MAGIC <strong style="color:#2e7d32;">Run the cell below</strong> to display your uploaded screenshot. Verify that it shows your updated masking and row filtering results.
# MAGIC </div>

# COMMAND ----------

show_evidence("evidence-02-masking.png", width=600)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B4. Create an ABAC Schema Policy
# MAGIC
# MAGIC Scale up your governance with Attribute-Based Access Control (ABAC). You will create a policy that automatically masks the `address` column in any table that has a `pii` tag.
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Step 1: Tag the address columns</strong>
# MAGIC <div style="color:#333;">Run the code cell below. It finds every table in your schema that has an <code>address</code> column and sets the tag <code>pii = address</code> on it.</div>
# MAGIC </div>

# COMMAND ----------

# Get the current username from the notebook context
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
# Extract the schema name from the username
schema = username.split('@')[0]

# Retrieve the list of tables in the specified schema
tables = [row.tableName for row in spark.sql(f"SHOW TABLES IN dbacademy.{schema}").collect()]
# Iterate through each table in the schema
for table in tables:
    # Get the list of columns for the current table
    columns = [row.col_name for row in spark.sql(f"DESCRIBE TABLE dbacademy.{schema}.{table}").collect()]
    # Check each column to find 'address'
    for col in columns:
        if col == "address":
            # Set the tag 'pii' to 'address' for the 'address' column
            spark.sql(f"ALTER TABLE dbacademy.{schema}.{table} ALTER COLUMN {col} SET TAGS ('pii' = 'address')")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B5. Create the Policy in the UI
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Step 2: Navigate to your schema</strong>
# MAGIC <div style="color:#333;">Open <strong>Catalog</strong> from the left sidebar. Navigate to <strong>dbacademy</strong> > <strong>your_schema</strong> (e.g., <code>labuser15359237_1780317248</code>). Click the <strong>Policies</strong> tab, then click <strong>New Policy</strong>.</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Step 3: Configure the policy basics</strong>
# MAGIC <div style="color:#333;">
# MAGIC <ol>
# MAGIC <li><strong>Name:</strong> <code>address_policy</code></li>
# MAGIC <li><strong>Description:</strong> Mask the address column in all tables within this schema.</li>
# MAGIC <li><strong>Applied to:</strong> All account users</li>
# MAGIC <li><strong>Except for:</strong> <code>metastore_admins</code></li>
# MAGIC <li><strong>Scope:</strong> <code>dbacademy.your_schema.All tables</code> (select your schema from the dropdown)</li>
# MAGIC </ol>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Step 4: Set the policy type and conditions</strong>
# MAGIC <div style="color:#333;">
# MAGIC <ol>
# MAGIC <li><strong>Policy type:</strong> Select <strong>Column mask</strong> (replaces column values with masked versions).</li>
# MAGIC <li><strong>Condition:</strong> Leave as <strong>No condition</strong> (all tables in scope).</li>
# MAGIC <li><strong>Column conditions:</strong> Choose one of these approaches:
# MAGIC   <ul>
# MAGIC     <li><strong>Option A (Tag matching):</strong> Select <strong>Columns matching any of these tags</strong>. Add the tag <code>pii : address</code>. The system generates: <code>hasTagValue('pii','address')</code>.</li>
# MAGIC     <li><strong>Option B (Custom expression):</strong> Select <strong>Columns matching a custom expression</strong>. Type: <code>hasTagValue('pii','address')</code></li>
# MAGIC   </ul>
# MAGIC </li>
# MAGIC </ol>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Step 5: Create the masking function</strong>
# MAGIC <div style="color:#333;">
# MAGIC <ol>
# MAGIC <li>In the <strong>Masking function</strong> section, toggle to <strong>Create</strong> (not "Select Existing").</li>
# MAGIC <li>Make sure the <strong>Shared Warehouse</strong> is selected and running in the top-right dropdown.</li>
# MAGIC <li>Click the <strong>AI assistant icon</strong> (sparkle icon on the right).</li>
# MAGIC <li>In the prompt field, type: <code>show first 3 characters</code></li>
# MAGIC <li>Click <strong>Generate</strong>. The assistant generates a function that preserves the first 3 characters and masks the rest.</li>
# MAGIC <li>Click <strong>Accept</strong> to apply the generated function.</li>
# MAGIC <li>Click <strong>Check syntax</strong> to validate. (If the button is greyed out, start the Shared Warehouse first.)</li>
# MAGIC <li>Expand <strong>Test function</strong>, enter a test value (e.g., <code>testingemail@email.com</code>), and click <strong>Run test</strong>. Verify the output shows the first 3 characters followed by asterisks (e.g., <code>tes*******************</code>).</li>
# MAGIC <li>Click <strong>Create policy</strong>.</li>
# MAGIC </ol>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #ff9800; background: #fff3e0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">Important</strong>
# MAGIC <div style="color:#333;">ABAC policies apply to SDP pipeline outputs (streaming tables and materialized views). If you plan to rerun the pipeline, either remove the policy first or add your <code>labuser@vocareum.com</code> to the exemption list to avoid refresh failures.</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">Step 6: Verify the policy</strong>
# MAGIC <div style="color:#333;">Run the code cells below to query tables with an <code>address</code> column. The address values should now show only the first 3 characters with the rest masked.</div>
# MAGIC </div>

# COMMAND ----------

# Display the churn_users_bronze table
display(spark.read.table(f"dbacademy.{schema}.churn_users_bronze"))

# Display the churn_features table excluding the address column
display(spark.read.table(f"dbacademy.{schema}.churn_features").select("address", *[col for col in spark.read.table(f"dbacademy.{schema}.churn_features").columns if col != "address"]))

# Display the churn_prediction table excluding the address column
display(spark.read.table(f"dbacademy.{schema}.churn_prediction").select("address", *[col for col in spark.read.table(f"dbacademy.{schema}.churn_prediction").columns if col != "address"]))
