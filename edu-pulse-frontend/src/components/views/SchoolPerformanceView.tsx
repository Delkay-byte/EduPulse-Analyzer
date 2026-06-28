import React from 'react';
import { Users, GraduationCap, TrendingUp, AlertCircle } from 'lucide-react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

// Mock data: When you build the backend, this useEffect will be a fetch call
const mockData = {
  kpis: [
    { label: 'Total Students', value: '432', icon: Users, color: 'text-blue-500' },
    { label: 'Pass Rate', value: '88.4%', icon: GraduationCap, color: 'text-emerald-500' },
    { label: 'Avg Aggregate', value: '14.2', icon: TrendingUp, color: 'text-purple-500' },
    { label: 'At Risk', value: '12', icon: AlertCircle, color: 'text-rose-500' },
  ],
  subjectPerformance: [
    { name: 'Math', score: 62 },
    { name: 'Eng', score: 75 },
    { name: 'Sci', score: 58 },
    { name: 'Soc', score: 81 },
    { name: 'ICT', score: 70 },
  ],
  auditData: [
    { id: '1001', name: 'Kofi Mensah', math: 85, eng: 72, sci: 68, aggregate: 12 },
    { id: '1002', name: 'Ama Serwaa', math: 42, eng: 55, sci: 48, aggregate: 28 },
    { id: '1003', name: 'Yaw Boateng', math: 78, eng: 80, sci: 82, aggregate: 09 },
  ]
};

export const SchoolPerformanceView = () => {
  return (
    <div className="space-y-8 animate-in fade-in duration-500">
      
      {/* 1. THE KPI BAR (render_school_dashboard) */}
      <div className="grid grid-cols-4 gap-6">
        {mockData.kpis.map((kpi, i) => (
          <div key={i} className="bg-slate-900 p-6 rounded-2xl border border-slate-800 flex items-center justify-between shadow-sm">
            <div>
              <p className="text-slate-400 text-xs font-bold uppercase tracking-wider">{kpi.label}</p>
              <h3 className="text-2xl font-black mt-1">{kpi.value}</h3>
            </div>
            <div className={`p-3 bg-slate-950 rounded-xl ${kpi.color}`}>
              <kpi.icon size={20} />
            </div>
          </div>
        ))}
      </div>

      {/* 2. SUBJECT ANALYTICS */}
      <div className="bg-slate-900 p-6 rounded-2xl border border-slate-800 h-72">
        <h3 className="text-lg font-bold mb-6">Subject Proficiency (%)</h3>
        <ResponsiveContainer width="100%" height="80%">
          <BarChart data={mockData.subjectPerformance}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
            <XAxis dataKey="name" stroke="#64748b" axisLine={false} tickLine={false} />
            <YAxis stroke="#64748b" axisLine={false} tickLine={false} />
            <Tooltip contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #1e293b' }} />
            <Bar dataKey="score" fill="#3b82f6" radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* 3. THE AUDIT TABLE (render_audit_table) */}
      <div className="bg-slate-900 rounded-2xl border border-slate-800 overflow-hidden">
        <div className="p-6 border-b border-slate-800 flex justify-between items-center">
          <h3 className="text-lg font-bold">Student Audit List</h3>
          <div className="flex gap-2">
            <input placeholder="Search records..." className="bg-slate-950 border border-slate-700 rounded-lg px-4 py-2 text-sm focus:outline-none focus:border-blue-500" />
            <button className="bg-slate-800 px-4 py-2 rounded-lg text-sm hover:bg-slate-700">Filter</button>
          </div>
        </div>
        
        <table className="w-full text-left text-sm text-slate-300">
          <thead className="bg-slate-950 text-slate-500 uppercase text-[10px] tracking-widest font-bold">
            <tr>
              <th className="p-4">ID</th>
              <th className="p-4">Name</th>
              <th className="p-4">Math</th>
              <th className="p-4">English</th>
              <th className="p-4">Science</th>
              <th className="p-4">Aggregate</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800">
            {mockData.auditData.map((student) => (
              <tr key={student.id} className="hover:bg-slate-800/50">
                <td className="p-4 font-mono">{student.id}</td>
                <td className="p-4 font-bold text-white">{student.name}</td>
                <td className="p-4">{student.math}</td>
                <td className="p-4">{student.eng}</td>
                <td className="p-4">{student.sci}</td>
                <td className="p-4 font-bold text-blue-500">{student.aggregate}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};
