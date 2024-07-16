import json
import asyncio
import ollama

class OllamaAPI:
    def __init__(self):
        self.client = ollama.AsyncClient(host='http://192.168.1.112:11434')
        self.generated_names = []  # List to store generated names

    async def generate_completion(self, model, prompt, existing_names):
        # Include the existing names in the prompt
        modified_prompt = prompt + "\nExisting Names: " + json.dumps(existing_names)
        messages = [{"role": "user", "content": modified_prompt}]
        response_parts = []

        async for part in await self.client.chat(model=model, messages=messages, stream=True, format="json"):
            if "message" in part:
                response_parts.append(part["message"]["content"])
                
        return "".join(response_parts)

    async def process_prompt(self, user_model_key, user_prompt):
        completion = await self.generate_completion(user_model_key, user_prompt, self.generated_names)
        self.generated_names.append(completion)  # Add the new name to the list
        print(completion)

async def main():
    ollama_api = OllamaAPI()
    USER_PROMPT = 'Provide a Unique Software Company Name in json format {"name": "<company_name>"}, return only json and nothing else'
    ITEMS_TO_RETURN = int(input("Enter number of items to return: "))
    print(USER_PROMPT)
    MODEL_NAME = str(input("Enter the model name: "))
    for _ in range(ITEMS_TO_RETURN):
        await ollama_api.process_prompt(MODEL_NAME, USER_PROMPT)

    # Write generated names to a JSON file
    with open("generated_names.json", "w", encoding="utf-8") as json_file:
        json.dump(ollama_api.generated_names, json_file, indent=4)

if __name__ == "__main__":
    asyncio.run(main())
