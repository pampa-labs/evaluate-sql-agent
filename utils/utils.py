from tqdm import tqdm

class CustomDatabase:
    def __init__(self, name, database, evaluation_dataset):
        self.name = name
        self.database = database
        self.evaluation_dataset = evaluation_dataset
        self.agent = None
    
    def run_queries(self):
        # self.evaluation_dataset.loc[:, 'query_result'] = self.evaluation_dataset['query'].apply(self.database.run)
        results = []
        for row in tqdm(self.evaluation_dataset.iterrows()):
            _, data = row
            res = self.database.run(data['query'])
            results.append(res)
        self.evaluation_dataset['query_result'] = results

    def parse_query_results(self, llm_chain):
        results = []
        for row in tqdm(self.evaluation_dataset.iterrows(),total=len(self.evaluation_dataset),desc=f"Parsing results for {self.name}"):
            _, data = row
            try:
                res = llm_chain.run(question=data['question'], query=data['query'], query_result=data['query_result'])
            except:
                res = "ERROR PARSING RESULT"
            results.append(res)
        self.evaluation_dataset['nl_result'] = results
        
    def run_agent(self):
        results = []
        for row in tqdm(self.evaluation_dataset.iterrows(),total=len(self.evaluation_dataset),desc=f"Running agent on DB {self.name}"):
            _, data = row
            res = self.agent.run(data['question'])
            results.append(res)
        self.evaluation_dataset['agent_results'] = results