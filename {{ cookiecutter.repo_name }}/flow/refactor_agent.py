import os
import sys
from datetime import datetime
from dotenv import load_dotenv
import requests


class RefactorAgent:
    def __init__(self):
        load_dotenv()
        self.token = os.getenv("FLOW_TOKEN")
        if not self.token:
            raise Exception("❌ FLOW_TOKEN não encontrado no .env")

        self.base_url = "https://flow..com/channels-service"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def build_prompt(self, content, filename):
        return f"""
                    # system:
                    Você é um Cientista de Dados sênior e referência técnica. Seu papel é refatorar código Python para torná-lo mais claro, organizado e aderente às melhores práticas da linguagem.

                    # user:
                    Refatore o código a seguir aplicando as seguintes diretrizes:

                    1. Mantenha a funcionalidade original  
                    2. Reestruture o código para melhor modularização e clareza  
                    3. Use boas práticas de codificação Python (PEP8)  
                    4. Adicione docstrings e comentários **em português** seguindo o padrão Google ou NumPy  
                    5. Organize notebooks (`.ipynb`) com células lógicas e limpas, sem execuções desnecessárias  
                    6. Faça nomeação adequada e descritiva de variáveis e funções  

                    7. No início do script, inclua um cabeçalho com:

                        - Comentário com o nome do script e a data da refatoração (comentado com `#`)
                        - Uma docstring logo abaixo com:
                            • O nome do script (repetido no início da docstring)
                            • Uma descrição clara em português da funcionalidade do script
                            • O que o script faz, como faz e com quais tecnologias

                        Exemplo esperado:

                        \"\"\"  
                        flow_credentials_retriever.py  

                        Script de autenticação automática no Flow .

                        Este script abre um navegador Chrome para que o usuário faça login no  
                        sistema Flow  e captura os cookies de autenticação necessários.  
                        Após capturar os cookies, o script salva as credenciais no arquivo .env.  
                        \"\"\"  

                    ⚠️ **Importante**:  
                    - Não retorne blocos ` ```python ` ou ` ``` `  
                    - Retorne **apenas o código refatorado cru**, sem explicações, títulos ou marcações Markdown  
                    - Ao final do script, inclua este comentário:

                    # ⚠️ Recomendação:
                    # Após a refatoração, revise e execute o código para garantir que o comportamento permaneceu o mesmo.
                    # Realize testes manuais ou automatizados sempre que possível.

                    <arquivo>
                    {content}
                    </arquivo>
                    """




    def request_refactor(self, prompt):
        url = f"{self.base_url}/v2/chat/messages"
        response = requests.post(url, headers=self.headers, json={
            "content": [{"type": "text/plain", "value": prompt}],
            "model": {
                "name": "claude37sonnet",
                "provider": "amazon-bedrock",
                "modelSettings": []
            },
            "agent": "chat-with-docs",
            "sources": [],
            "connectors": [],
            "operation": "new-question"
        })

        if response.status_code != 200:
            raise Exception(f"Erro na requisição: {response.text}")

        chat_id = response.text.strip().split("\n")[0]
        chat_id = eval(chat_id).get("chatId")
        if not chat_id:
            raise Exception("chatId não encontrado na resposta.")

        # Pegar resposta final
        response_final = requests.get(
            f"{self.base_url}/v1/chat/{chat_id}/messages",
            headers=self.headers
        )

        messages = response_final.json().get("messages", [])
        content = ""

        for msg in messages:
            for item in msg.get("metadata", {}).get("content", []):
                if item.get("author") == "assistant":
                    content += item.get("value", "")

        return content


    def process_directory(self, input_dir: str, output_dir: str):
        os.makedirs(output_dir, exist_ok=True)
        count = 0

        for root, _, files in os.walk(input_dir):
            for file in files:
                if not file.endswith((".py", ".ipynb")) or file.startswith("."):
                    continue

                input_path = os.path.join(root, file)
                filename_no_ext, ext = os.path.splitext(file)
                output_filename = f"{filename_no_ext}_refatorado{ext}"
                output_path = os.path.join(output_dir, output_filename)

                with open(input_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                print(f"🔧 Refatorando: {file}")
                prompt = self.build_prompt(content, file)
                refactored_code = self.request_refactor(prompt)

                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write("# 📄 Refatorado automaticamente via Flow\n")
                    f.write(f"# ⏱ Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n\n")
                    f.write(refactored_code)

                print(f"✅ Salvo em: {output_path}")
                count += 1

        print(f"\n✅ Refatoração concluída para {count} arquivo(s).")



if __name__ == "__main__":
    print("📁 Informe o caminho da pasta com os arquivos a refatorar:")
    input_dir = input(">> ").strip()

    print("📂 Informe a pasta de saída para os arquivos refatorados:")
    output_dir = input(">> ").strip()
    
    if not os.path.isdir(input_dir):
        print(f"❌ Diretório inválido: {input_dir}")
        sys.exit(1)

    agent = RefactorAgent()
    agent.process_directory(input_dir, output_dir)
