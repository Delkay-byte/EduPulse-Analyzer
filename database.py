from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, DateTime, JSON
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from datetime import datetime

# TODO: Replace 'postgres' and 'yourpassword' with your actual local pgAdmin credentials
DATABASE_URL = "postgresql://postgres:143%40DelKay@localhost:5432/edupulse_db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class School(Base):
    __tablename__ = "schools"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True, nullable=False)
    circuit = Column(String, index=True, nullable=False)
    school_type = Column(String, default="Public")
    
    # Relationship to link students and users back to this school
    students = relationship("Student", back_populates="school")
    users = relationship("User", back_populates="school")

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    role = Column(String, nullable=False) # 'Director' or 'Headteacher'
    school_id = Column(Integer, ForeignKey("schools.id"), nullable=True) # Null if Director
    
    school = relationship("School", back_populates="users")

class Student(Base):
    __tablename__ = "students"
    id = Column(Integer, primary_key=True, index=True)
    waec_id = Column(String(10), unique=True, index=True, nullable=False)
    full_name = Column(String, nullable=False)
    gender = Column(String(1), nullable=False) # 'M' or 'F'
    dob = Column(String, nullable=False)
    attendance_percent = Column(Float, nullable=True)
    school_id = Column(Integer, ForeignKey("schools.id"))

    # Extracted Official WAEC Grades (Stored as strings to prevent float crashes)
    math_waec = Column(String, nullable=True)
    english_waec = Column(String, nullable=True)
    science_waec = Column(String, nullable=True)
    social_waec = Column(String, nullable=True)
    ict_waec = Column(String, nullable=True)
    rme_waec = Column(String, nullable=True)
    bdt_waec = Column(String, nullable=True)
    french_waec = Column(String, nullable=True)
    ewe_waec = Column(String, nullable=True)
    
    school = relationship("School", back_populates="students")
    academic_features = relationship("AcademicRecord", back_populates="student", uselist=False)

class AcademicRecord(Base):
    __tablename__ = "academic_records"
    """
    Instead of creating 54 individual columns for Term 1, Term 2, Mocks, etc., 
    we use a JSONB column. This makes the database insanely fast and perfectly 
    scalable. You just dump the Streamlit dataframe row straight into this column.
    """
    id = Column(Integer, primary_key=True, index=True)
    student_id = Column(Integer, ForeignKey("students.id"), unique=True)
    
    # Stores the raw mock, assignment, and term scores used by the 9 ML models
    raw_scores = Column(JSON, nullable=True) 
    
    # Stores the ML predictions so they don't have to be recalculated
    ml_predictions = Column(JSON, nullable=True)
    
    student = relationship("Student", back_populates="academic_features")

# --- INITIALIZE THE DATABASE ---
def init_db():
    Base.metadata.create_all(bind=engine)
    print("✅ PostgreSQL Database tables created successfully!")

if __name__ == "__main__":
    init_db()