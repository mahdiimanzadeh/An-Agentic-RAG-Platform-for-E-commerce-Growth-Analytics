from src.database.manager import DatabaseManager
from src.llm.schema_generator import generate_schema_description
from src.llm.prompts import get_system_prompt

def main():

    db_manager = DatabaseManager()
    if not db_manager.connect():
        print("Could not connect to database.")
        return

    #  Generate Schema Context
    print("Generating Schema Description...")
    schema_context = generate_schema_description(db_manager.engine)
    
    # Generate Full System Prompt
    print("Constructing System Prompt...")
    full_prompt = get_system_prompt(schema_context)
    
    output_file = "generated_system_prompt.txt"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(full_prompt)
        
    print(f"\nSuccess! The full system prompt has been saved to '{output_file}'.")

if __name__ == "__main__":
    main()
