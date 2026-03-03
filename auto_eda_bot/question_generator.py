def generate_questions(df):
    questions = []
    
    numeric_cols = df.select_dtypes(include='number').columns
    cat_cols = df.select_dtypes(include='object').columns
    
    for col in numeric_cols:
        questions.append(f"What is the distribution of {col}?")
        questions.append(f"What is the average of {col}?")
    
    for col in cat_cols:
        questions.append(f"Which {col} category appears most frequently?")
    
    return questions