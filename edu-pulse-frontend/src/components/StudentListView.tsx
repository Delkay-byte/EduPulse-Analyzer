import { Search, Filter, MoreVertical, AlertCircle } from 'lucide-react';

export default function StudentListView() {
  // Mock data representing the parsed Excel upload
  const students = [
    { id: '10501001', name: 'John Doe', grade: 'Grade 9', attendance: 95, atRisk: false, avgScore: 82 },
    { id: '10501002', name: 'Jane Smith', grade: 'Grade 9', attendance: 88, atRisk: true, avgScore: 45 },
    { id: '10501003', name: 'Samuel Osei', grade: 'Grade 8', attendance: 92, atRisk: false, avgScore: 78 },
    { id: '10501004', name: 'Ama Mensah', grade: 'Grade 8', attendance: 75, atRisk: true, avgScore: 50 },
    { id: '10501005', name: 'Kwame Nkrumah', grade: 'Grade 7', attendance: 98, atRisk: false, avgScore: 91 },
  ];

  return (
    <div className="space-y-6 animate-fadeIn">
      {/* Header & Controls */}
      <div className="flex justify-between items-end">
        <div>
          <h2 className="text-3xl font-black text-white">Student Roster</h2>
          <p className="text-slate-400 mt-1">Manage and monitor academic performance.</p>
        </div>
        
        <div className="flex gap-4">
          <div className="relative">
            <Search size={18} className="absolute left-3 top-3 text-slate-500" />
            <input 
              type="text" 
              placeholder="Search by name or ID..." 
              className="bg-slate-900 border border-slate-700 text-white rounded-lg pl-10 pr-4 py-2 w-64 focus:border-blue-500 outline-none"
            />
          </div>
          <button className="bg-slate-900 border border-slate-700 text-white px-4 py-2 rounded-lg flex items-center gap-2 hover:bg-slate-800 transition-colors">
            <Filter size={18} /> Filters
          </button>
        </div>
      </div>

      {/* The Data Table */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl overflow-hidden shadow-lg">
        <table className="w-full text-left text-sm text-slate-300">
          <thead className="bg-slate-950 text-slate-500 uppercase text-[10px] tracking-widest font-bold">
            <tr>
              <th className="p-4">Student ID</th>
              <th className="p-4">Name</th>
              <th className="p-4">Class</th>
              <th className="p-4">Attendance</th>
              <th className="p-4">Avg Score</th>
              <th className="p-4">Status</th>
              <th className="p-4 text-right">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/50">
            {students.map((student) => (
              <tr key={student.id} className="hover:bg-slate-800/50 transition-colors">
                <td className="p-4 font-mono text-xs">{student.id}</td>
                <td className="p-4 font-bold text-white">{student.name}</td>
                <td className="p-4">{student.grade}</td>
                <td className="p-4">
                  <span className={`${student.attendance < 80 ? 'text-amber-500 font-bold' : ''}`}>
                    {student.attendance}%
                  </span>
                </td>
                <td className="p-4">{student.avgScore}%</td>
                <td className="p-4">
                  {student.atRisk ? (
                    <span className="flex items-center gap-1 text-rose-500 bg-rose-500/10 px-2 py-1 rounded w-fit text-xs font-bold">
                      <AlertCircle size={14} /> At Risk
                    </span>
                  ) : (
                    <span className="text-emerald-500 bg-emerald-500/10 px-2 py-1 rounded w-fit text-xs font-bold">
                      On Track
                    </span>
                  )}
                </td>
                <td className="p-4 text-right">
                  <button className="text-slate-500 hover:text-white p-1">
                    <MoreVertical size={18} />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
