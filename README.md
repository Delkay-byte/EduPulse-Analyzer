📘 EduPulse Architecture Blueprint (v1.0)
Current Phase: Backend Database Migration
Tech Stack: PostgreSQL, SQLAlchemy (ORM), Python
Local Database Name: edupulse_db

Core Database Schema:

users: Manages authentication and roles (Director vs. Headteacher).

schools: Stores the metadata mapping JHS names to their respective circuits.

students: The master table for demographics, attendance, and WAEC grades.

academic_records: A specialized table to handle the term exams, mocks, and assignments for the 9 decoupled ML models.