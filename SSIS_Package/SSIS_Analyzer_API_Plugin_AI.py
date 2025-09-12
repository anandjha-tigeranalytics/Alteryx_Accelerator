import os
import json
import requests

from dotenv import load_dotenv

 # Load variables from .env into environment
load_dotenv()
# Access the variable
api_key = os.getenv("GEMINI_API_KEY")

def read_package_file(file_path):
    """
    Reads the content of a .dtsx file.

    Args:
        file_path (str): The path to the .dtsx file.

    Returns:
        str: The content of the .dtsx file, or None if an error occurs.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return content
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return None
    except Exception as e:
        print(f"Error reading file '{file_path}': {e}")
        return None


def write_to_file(file_path, content):
    """
    Writes the content to a text file.

    Args:
        file_path (str): The path to the output text file.
        content (str): The content to write.
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    except Exception as e:
        print(f"Error writing to file '{file_path}': {e}")
        return False


def call_gemini_api(payload, api_key):
    """
    Makes a POST request to the Gemini API and returns the extracted text.

    Args:
        payload (dict): The payload to send to the API.
        api_key (str): Your Gemini API key.

    Returns:
        str: The extracted text content from the API response, or None on failure.
    """
    if not api_key:
        print("Error: API key is missing.")
        return None

    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}"


    try:
        response = requests.post(
            api_url,
            headers={'Content-Type': 'application/json'},
            data=json.dumps(payload)
        )
        response.raise_for_status()
        result = response.json()

        # Extract and clean the text from the response
        text_content = result["candidates"][0]["content"]["parts"][0]["text"]
        text_content = text_content.strip()
        if text_content.startswith("```"):
            text_content = text_content[3:].strip()

        if text_content.endswith("```"):
            text_content = text_content[:-3].strip()

        return text_content

    except requests.exceptions.RequestException as e:
        print(f"Error calling Gemini API: {e}")
    except (json.JSONDecodeError, KeyError, IndexError, AttributeError):
        print("Error processing response from Gemini API.")
        print(f"Received Response: {response.text}")
    return None


def generate_ssis_summary(package_path,output_folder):
    packageContent = read_package_file(package_path)
    """
    Generates a summary of a SSIS Package using the Gemini API.

    """
    job_name = os.path.splitext(packageContent)[0]
    
    
    prompt1 = f"""
             Analyze the following DataStage job design (from file '{packageContent}'. " \
             Generate a structured summary in Markdown format. The summary must include the following sections:\n\n" \
             1.  **Overall Purpose**: A brief, high-level description of what the job accomplishes.\n" \
             2.  **Data Flow**: Describe the journey of the data from source(s) to target(s).\n" \
             3.  **Key Stages and Transformations**: Use a bulleted list to detail the main processing steps (e.g., " \
                   lookups, aggregations, transformations, filtering).\n" \
             4.  **Inputs**: Use a bulleted list to identify the primary data sources (e.g., file paths, table names).\n" \
             5.  **Outputs**: Use a bulleted list to identify the final data targets.\n\n" \
             Here is the job content:\n" \
             """
    prompt2 = f"""
    
             Analyze the following DataStage job design (from file '{packageContent}'. " \
             Generate a structured summary in Markdown format. The summary must include the following sections:\n\n" \
             1.  **Overall Purpose**: A brief, high-level description of what the job accomplishes.\n" \
             2.  **Data Flow**: Describe the journey of the data from source(s) to target(s).\n" \
             3.  **Key Stages and Transformations**: Use a bulleted list to detail the main processing steps (e.g., " \
             f"lookups, aggregations, transformations, filtering).\n" \
             4.  **Summarize Control Flow and Data Flow** .\n" \
             5.  ** Are there any Loops Involved,If So Explain their function**.\n" \
             6. ** Explain How error handling is Implemented**.\n"\
             7. **Check if any Event Handlers involved,if so explain in detail**.\n"\
             8. **Extract all SQL Commands or queries used in execute sql task**.\n"\
    """

    payload = {"contents": [{"role": "user", "parts": [{"text": prompt2}]}]}

    return call_gemini_api(payload, api_key)




    payload = {"contents": [{"role": "user", "parts": [{"text": prompt}]}]}

    content =  call_gemini_api(payload, api_key)

    fileName = package_path.split("\\")[-1].split(".")[0]
    output_folder = os.path.join(output_folder , "SSIS_Summary")
    os.makedirs(output_folder,exist_ok=True)
    output_folder = os.path.join(output_folder , fileName+".txt")
    flag = False
    flag = write_to_file(output_folder,content)
    if(flag):
        print(f"Summary is generated for {package_path}")
