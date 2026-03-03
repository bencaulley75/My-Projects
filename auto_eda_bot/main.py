import pandas as pd
from cleaner import auto_clean
from question_generator import generate_questions

# Load dataset
df = pd.read_csv("sample_data.csv")

print("Original Shape:", df.shape)

# Clean data
df = auto_clean(df)

print("Cleaned Shape:", df.shape)

# Generate questions
questions = generate_questions(df)

print("\nGenerated Business Questions:")
for q in questions:
    print("-", q)