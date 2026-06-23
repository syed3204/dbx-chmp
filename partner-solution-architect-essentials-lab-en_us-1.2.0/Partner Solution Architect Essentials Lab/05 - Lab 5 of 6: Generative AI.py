# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 5 of 6: Generative AI
# MAGIC
# MAGIC Run the Generative AI notebooks, then complete the activities below. You will use LLMs, build agents, and evaluate AI-generated content.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Execute and Modify Generative AI Notebooks
# MAGIC
# MAGIC ### A1. Review the Generative AI Notebooks
# MAGIC
# MAGIC 1. **Open and Execute the Notebooks**
# MAGIC    - Review the notebook **05.1-Agent-Functions-Creation** section that was run during the setup job.
# MAGIC    - **This created the three functions** for gathering information, building prompts, and interacting with models.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A2. Deploy Your Agent as a Databricks App
# MAGIC
# MAGIC Follow these steps to build and deploy an AI agent using the Databricks Playground:
# MAGIC
# MAGIC **Step 1: Open the Playground**
# MAGIC - Navigate to the **Playground** from the left sidebar.
# MAGIC
# MAGIC **Step 2: Select an LLM**
# MAGIC - Use the model dropdown at the top left and select an LLM with tool support.
# MAGIC
# MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="color: #c62828;">Recommended: GPT-5 Mini</strong>
# MAGIC <p style="margin: 8px 0 0 0; color: #333;">Not all LLMs perform equally well as agents. Smaller models like <strong>Mini</strong> and <strong>Nano</strong> variants often work exceptionally well for tool-calling agents. Always test your chosen LLM in the Playground before deploying it as an app.</p>
# MAGIC </div>
# MAGIC
# MAGIC **Step 3: Attach Your UC Functions as Tools**
# MAGIC - Click the **Tools** dropdown next to the model selector.
# MAGIC - Click **+ Add tool** to open the tool selection dialog.
# MAGIC - In the **Add tools** dialog, click the **MCP Servers** tab.
# MAGIC - Under **Unity Catalog Function MCP Server**, click **Browse** next to "Choose UC function".
# MAGIC - In the **Select a UC function** dialog, click the **All** tab to see all available functions.
# MAGIC - Navigate to your schema: **All catalogs** > **dbacademy** > **your_schema** (e.g., `labuser12345`).
# MAGIC - Select all three functions:
# MAGIC   - `churn_predictor`
# MAGIC   - `customer_order_lookup`
# MAGIC   - `generate_marketing_copy`
# MAGIC - Click **Confirm**, then click **Save** to attach the tools.
# MAGIC
# MAGIC **Step 4: Test the Agent in the Playground**
# MAGIC - Try a prompt like: `Please generate market copy of user_id = 2d17d7cd-38ae-440d-8485-34ce4f8f3b46`
# MAGIC - Verify the agent calls your functions and returns a response.
# MAGIC
# MAGIC **Step 5: Export to Databricks Apps**
# MAGIC - Click the **Get code** dropdown (next to the Tools dropdown).
# MAGIC - Select **"Export to Databricks Apps"** (Recommended).
# MAGIC - In the export dialog:
# MAGIC   - **App Name**: Enter a unique name (e.g., `agent-labuser12345`).
# MAGIC   - **App Description**: (Optional) Describe the agent.
# MAGIC   - **MLflow Experiment**: Click **+ Create new experiment** and name it to match your app (e.g., `agent-labuser12345`).
# MAGIC   - Leave **On-Behalf-Of (OBO) authentication** unchecked unless directed otherwise.
# MAGIC - Click **Export** to create the Databricks App.
# MAGIC
# MAGIC **Step 6: View Agent and Wait for Deployment**
# MAGIC - After the export completes, you will see a success message ("Exported agent successfully!"). Click **View Agent**.
# MAGIC - This opens the **Databricks Apps** page for your agent. You will see:
# MAGIC   - **App status**: "Compute is starting" -- wait for compute to become ready (2-3 minutes).
# MAGIC   - **Deployment history**: "Deployment will be initiated once app compute is active."
# MAGIC - Once compute is ready, the app will deploy and install packages automatically.
# MAGIC - Wait until the **App status** shows **Running** with a green indicator and a URL link.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A3. Verification: Agent Deployment and Interaction
# MAGIC
# MAGIC <div style="border-left: 4px solid #4caf50; background: #e8f5e9; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#2e7d32; margin-bottom:6px; font-size: 1.1em;">Verification</strong>
# MAGIC <div style="color:#333;">Provide evidence that you have successfully deployed the agent as a Databricks App and interacted with it.</div>
# MAGIC </div>
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. **Open the App:**
# MAGIC    - Once the App status shows **Running** (green indicator), click the **app URL** in the App status section.
# MAGIC    - The app opens a chat interface with "What would you like to know?"
# MAGIC
# MAGIC 2. **Interact with the Agent in the App:**
# MAGIC    - Test the agent with a prompt like: `Please generate market copy of user_id = 2d17d7cd-38ae-440d-8485-34ce4f8f3b46`
# MAGIC    - The agent will call your UC functions (churn prediction, order lookup, marketing copy generation) and return a personalized response.
# MAGIC
# MAGIC 3. **Save** a screenshot of the **agent's response** in the app's chat interface.
# MAGIC 4. Click the **Upload Screenshot** button in the widget below, navigate to your saved screenshot, and select **Open**.
# MAGIC 5. Run the `show_evidence` code cell to verify the image appears correctly.

# COMMAND ----------

# MAGIC %run ./Includes/Images/Image_Display_Function

# COMMAND ----------

upload_evidence("evidence-05-agent.png")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #ff9800; background: #fff3e0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">Note</strong>
# MAGIC <div style="color:#333;">If the upload widget does not appear after 30 seconds, re-run the cell above. Repeat until the widget appears.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4caf50; background: #e8f5e9; padding: 10px 14px; border-radius: 4px; margin: 12px 0;">
# MAGIC <strong style="color:#2e7d32;">Run the cell below</strong> to display your uploaded screenshot. Verify that it shows the agent's response in the Databricks App chat interface.
# MAGIC </div>

# COMMAND ----------

show_evidence("evidence-05-agent.png", width=600)
