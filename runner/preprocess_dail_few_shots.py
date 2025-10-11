import json
from typing import Dict, Any
from argparse import ArgumentParser
import re


def extract_question_section(text):
    """
    Extract question-sql pairs from the prompt text.
    Returns a list of dictionaries with 'question' and 'sql' keys.
    """
    examples = []
    
    # Find all /* Answer the following: ... */ patterns
    pattern = r'/\* Answer the following:\s*(.*?)\s*\*/\s*(.*?)(?=/\*|$)'
    matches = re.findall(pattern, text, re.DOTALL)
    
    for question, sql in matches[:-1]:
        # Clean up the question
        question = question.strip()
        
        # Clean up the SQL - remove leading/trailing whitespace and newlines
        sql = sql.strip()
        
        # Remove any trailing semicolons and extra whitespace
        sql = re.sub(r';\s*$', '', sql).strip()
        
        # Only add if both question and SQL are not empty
        if question and sql:
            examples.append({
                'question': question,
                'sql': sql
            })
    
    return examples
    


def preprocess_dail_few_shots(dail_sql_input_path: str, output_path: str):
    with open(dail_sql_input_path, "r") as f:
        dail_data = json.load(f)
    dail_inputs = dail_data["questions"]
    output = {}
    for question_id, dail_input in enumerate(dail_inputs):
        prompt = dail_input["prompt"]
        examples = extract_question_section(prompt)
        output[question_id] = examples
    with open(output_path, "w") as f:
        json.dump(output, f, indent=4)
        

def main():
    parser = ArgumentParser()
    parser.add_argument("--dail_sql_input_path", type=str, required=True)
    parser.add_argument("--output_path", type=str, required=True)
    args = parser.parse_args()
    preprocess_dail_few_shots(args.dail_sql_input_path, args.output_path)

    
if __name__ == "__main__":
    main()