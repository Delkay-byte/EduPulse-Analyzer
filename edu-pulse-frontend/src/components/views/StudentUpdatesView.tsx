import React, { useState } from 'react';
import { Save, User, BookOpen } from 'lucide-react';

export const StudentUpdatesView = () => {
  // Keeping track of the form state here. 
  // When we connect the backend, this object will be sent directly to our POST endpoint.
  const [formData, setFormData] = useState({
    studentId: '',
    name: '',
    gender: '',
    dob: '',
    // Just a sample of the subjects from your template
    mathScore: '',
    engScore: '',
    sciScore: '',
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    // This is where we'll eventually trigger the API call to save to PostgreSQL
    console.log("Saving student record:", formData);
    alert("Student record saved successfully!");
  };

  return (
    <div className="space-y-6 animate-in fade-in duration-500">
      <div>
        <h2 className="text-2xl font-black text-white">Manual Student Entry</h2>
        <p className="text-slate-400">Add or update individual student records to the system.</p>
      </div>

      <form onSubmit={handleSubmit} className="bg-slate-900 border border-slate-800 rounded-2xl p-8 space-y-8">
        
        {/* SECTION 1: BASIC INFO */}
        <div className="space-y-4">
          <div className="flex items-center gap-2 text-blue-500">
            <User size={20} />
            <h3 className="font-bold uppercase tracking-wider text-sm">Basic Information</h3>
          </div>
          <div className="grid grid-cols-2 gap-6">
            <input 
              placeholder="Student ID" 
              className="bg-slate-950 border border-slate-800 p-3 rounded-xl focus:border-blue-500 outline-none"
              onChange={(e) => setFormData({...formData, studentId: e.target.value})}
            />
            <input 
              placeholder="Full Name" 
              className="bg-slate-950 border border-slate-800 p-3 rounded-xl focus:border-blue-500 outline-none"
              onChange={(e) => setFormData({...formData, name: e.target.value})}
            />
          </div>
        </div>

        {/* SECTION 2: ACADEMIC SCORES */}
        <div className="space-y-4 pt-6 border-t border-slate-800">
          <div className="flex items-center gap-2 text-emerald-500">
            <BookOpen size={20} />
            <h3 className="font-bold uppercase tracking-wider text-sm">Academic Performance</h3>
          </div>
          <div className="grid grid-cols-3 gap-6">
            <div className="space-y-1">
              <label className="text-xs text-slate-500">Mathematics Score</label>
              <input 
                type="number" 
                className="w-full bg-slate-950 border border-slate-800 p-3 rounded-xl focus:border-emerald-500 outline-none"
                onChange={(e) => setFormData({...formData, mathScore: e.target.value})}
              />
            </div>
            <div className="space-y-1">
              <label className="text-xs text-slate-500">English Language</label>
              <input 
                type="number" 
                className="w-full bg-slate-950 border border-slate-800 p-3 rounded-xl focus:border-emerald-500 outline-none"
                onChange={(e) => setFormData({...formData, engScore: e.target.value})}
              />
            </div>
            <div className="space-y-1">
              <label className="text-xs text-slate-500">Integrated Science</label>
              <input 
                type="number" 
                className="w-full bg-slate-950 border border-slate-800 p-3 rounded-xl focus:border-emerald-500 outline-none"
                onChange={(e) => setFormData({...formData, sciScore: e.target.value})}
              />
            </div>
          </div>
        </div>

        {/* SUBMIT BUTTON */}
        <div className="pt-6 border-t border-slate-800 flex justify-end">
          <button 
            type="submit" 
            className="flex items-center gap-2 bg-blue-600 hover:bg-blue-500 px-8 py-3 rounded-xl font-bold transition-all"
          >
            <Save size={18} /> Save Student Record
          </button>
        </div>
      </form>
    </div>
  );
};
