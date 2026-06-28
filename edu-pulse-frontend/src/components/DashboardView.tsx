import { useState } from 'react';
import { 
  Users, UserCheck, BookOpen, Activity, Plus, 
  Bell, Clock, Server, Shield, CheckCircle, Calendar, X, ChevronLeft, ChevronRight
} from 'lucide-react';
import { 
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  PieChart, Pie, Cell
} from 'recharts';

export default function DashboardView({ onSwitchToDirector }: { onSwitchToDirector: () => void }) {
  const [isCalendarOpen, setIsCalendarOpen] = useState(false);

  // I am mocking the data here so the layout renders perfectly the moment I bypass the verify screen.
  const performanceData = [
    { name: 'Grade 7', Math: 82, Science: 78, English: 85 },
    { name: 'Grade 8', Math: 75, Science: 80, English: 79 },
    { name: 'Grade 9', Math: 88, Science: 85, English: 90 },
  ];

  const demographicData = [
    { name: 'Boys', value: 420 },
    { name: 'Girls', value: 430 },
  ];
  const COLORS = ['#3b82f6', '#ec4899']; // Blue for boys, Pink for girls

  // I like to keep my data structures flat and predictable. 
  // Later, when we connect a real database, the API just needs to return JSON that matches this exact shape.
  const pendingTasks = [
    { id: 1, title: 'Review Grade 9 Science Mock Exams', deadline: 'Today, 2:00 PM', priority: 'High' },
    { id: 2, title: 'Approve Term 2 Budget Proposal', deadline: 'Tomorrow', priority: 'Medium' },
    { id: 3, title: 'Submit Monthly Attendance Report', deadline: 'Tomorrow', priority: 'High' },
    { id: 4, title: 'Review PTA Meeting Minutes', deadline: 'Friday', priority: 'Low' },
  ];

  const upcomingEvents = [
    { id: 1, title: 'Mid-Term Examinations Start', date: 'June 20, 2026', type: 'Academic' },
    { id: 2, title: 'District Director Site Visit', date: 'June 24, 2026', type: 'Administrative' },
    { id: 3, title: 'Republic Day Holiday', date: 'July 1, 2026', type: 'Holiday' },
  ];

  return (
    // Main content area only - sidebar is now in HeadteacherPortal
    <div className="p-8 space-y-8">
        
        {/* HEADER */}
        <header className="flex justify-between items-end">
          <div>
            <h2 className="text-3xl font-black">Welcome back, Sarah!</h2>
            <p className="text-slate-400 mt-1">Here is what is happening at your school today.</p>
          </div>
          <button className="p-2 bg-slate-800 rounded-full hover:bg-slate-700 transition-colors">
            <Bell size={20} />
          </button>
        </header>

        {/* SECTION 1: AT A GLANCE (KPIs) */}
        <div className="grid grid-cols-4 gap-6">
          {[
            { label: 'Total Students', value: '850', icon: Users, color: 'text-blue-500' },
            { label: 'Avg Attendance', value: '92%', icon: UserCheck, color: 'text-emerald-500' },
            { label: 'Teachers', value: '45', icon: BookOpen, color: 'text-purple-500' },
            { label: 'Active Classes', value: '24', icon: Activity, color: 'text-orange-500' }
          ].map((stat, i) => (
            <div key={i} className="bg-slate-900 p-6 rounded-2xl border border-slate-800 flex items-start justify-between">
              <div>
                <p className="text-slate-400 text-sm font-medium mb-1">{stat.label}</p>
                <h3 className="text-3xl font-black">{stat.value}</h3>
              </div>
              <div className={`p-3 bg-slate-950 rounded-xl ${stat.color}`}>
                <stat.icon size={24} />
              </div>
            </div>
          ))}
        </div>

        {/* SECTION 2: QUICK ACTIONS */}
        <div className="bg-slate-900 p-6 rounded-2xl border border-slate-800">
          <h3 className="text-lg font-bold mb-4">Quick Actions</h3>
          <div className="grid grid-cols-4 gap-4">
            {['Add Student', 'Add Teacher', 'Attendance', 'Reports'].map((action, i) => (
              <button key={i} className="flex flex-col items-center justify-center gap-2 p-4 bg-slate-950 rounded-xl border border-slate-800 hover:border-blue-500 transition-colors">
                <Plus size={20} className="text-blue-500" />
                <span className="text-sm font-medium">{action}</span>
              </button>
            ))}
          </div>
        </div>

        {/* SECTION 2.5: TASKS & EVENTS */}
        <div className="grid grid-cols-2 gap-6">
          
          {/* Pending Tasks Panel */}
          <div className="bg-slate-900 p-6 rounded-2xl border border-slate-800 flex flex-col h-80">
            <div className="flex justify-between items-center mb-6">
              <h3 className="text-lg font-bold">Pending Tasks</h3>
              <span className="bg-rose-500/10 text-rose-500 text-xs font-bold px-2 py-1 rounded-full">
                {pendingTasks.filter(t => t.priority === 'High').length} High Priority
              </span>
            </div>
            
            <div className="space-y-3 overflow-y-auto pr-2">
              {/* I'm mapping through the array here. The UI just repeats for however many tasks exist. */}
              {pendingTasks.map(task => (
                <div key={task.id} className="flex items-start gap-4 p-3 hover:bg-slate-800/50 rounded-xl transition-colors group cursor-pointer border border-transparent hover:border-slate-700">
                  <div className="mt-1">
                    <CheckCircle size={18} className="text-slate-600 group-hover:text-emerald-500 transition-colors" />
                  </div>
                  <div className="flex-1">
                    <p className="text-sm font-medium text-white">{task.title}</p>
                    <p className="text-xs text-slate-400 mt-1 flex items-center gap-1">
                      <Clock size={12} /> {task.deadline}
                    </p>
                  </div>
                  {/* Dynamic coloring based on the data's priority level */}
                  <div className={`text-[10px] font-bold uppercase tracking-wider px-2 py-1 rounded ${
                    task.priority === 'High' ? 'bg-rose-500/20 text-rose-400' : 
                    task.priority === 'Medium' ? 'bg-amber-500/20 text-amber-400' : 
                    'bg-slate-800 text-slate-400'
                  }`}>
                    {task.priority}
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Upcoming Events Panel */}
          <div className="bg-slate-900 p-6 rounded-2xl border border-slate-800 flex flex-col h-80">
            <div className="flex justify-between items-center mb-6">
              <h3 className="text-lg font-bold">Upcoming Events</h3>
              <button onClick={() => setIsCalendarOpen(true)} className="text-blue-500 text-sm hover:text-blue-400 font-medium">View Calendar</button>
            </div>

            <div className="space-y-4 overflow-y-auto pr-2">
              {upcomingEvents.map(event => (
                <div key={event.id} className="flex items-center gap-4 p-3 bg-slate-950 rounded-xl border border-slate-800 border-l-4 hover:border-l-blue-500 transition-colors">
                  <div className="bg-slate-900 p-3 rounded-lg text-blue-500">
                    <Calendar size={20} />
                  </div>
                  <div>
                    <p className="text-sm font-bold text-white">{event.title}</p>
                    <p className="text-xs text-slate-400 mt-1">{event.date}</p>
                  </div>
                </div>
              ))}
            </div>
          </div>

        </div>

        {/* SECTION 3: PERFORMANCE & DEMOGRAPHICS */}
        <div className="grid grid-cols-3 gap-6">
          <div className="col-span-2 bg-slate-900 p-6 rounded-2xl border border-slate-800 h-96">
            <h3 className="text-lg font-bold mb-6">Academic Performance Overview</h3>
            <ResponsiveContainer width="100%" height="80%">
              <BarChart data={performanceData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                <XAxis dataKey="name" stroke="#64748b" axisLine={false} tickLine={false} />
                <YAxis stroke="#64748b" axisLine={false} tickLine={false} />
                <Tooltip contentStyle={{ backgroundColor: '#0f172a', borderColor: '#1e293b' }} />
                <Legend />
                <Bar dataKey="Math" fill="#3b82f6" radius={[4, 4, 0, 0]} />
                <Bar dataKey="Science" fill="#10b981" radius={[4, 4, 0, 0]} />
                <Bar dataKey="English" fill="#8b5cf6" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
          
          <div className="bg-slate-900 p-6 rounded-2xl border border-slate-800 h-96">
            <h3 className="text-lg font-bold mb-2">Student Demographics</h3>
            <ResponsiveContainer width="100%" height="80%">
              <PieChart>
                <Pie data={demographicData} innerRadius={60} outerRadius={80} paddingAngle={5} dataKey="value">
                  {demographicData.map((_entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip contentStyle={{ backgroundColor: '#0f172a', borderColor: '#1e293b' }} />
              </PieChart>
            </ResponsiveContainer>
            <div className="flex justify-center gap-4 text-sm">
              <span className="flex items-center gap-2"><div className="w-3 h-3 rounded-full bg-blue-500"></div> Boys (49%)</span>
              <span className="flex items-center gap-2"><div className="w-3 h-3 rounded-full bg-pink-500"></div> Girls (51%)</span>
            </div>
          </div>
        </div>

        {/* SECTION 4: SYSTEM STATUS (The Footer logs) */}
        <div className="grid grid-cols-3 gap-6">
          {[
            { label: 'Server Status', value: 'Online', icon: Server, color: 'text-emerald-500' },
            { label: 'Last Backup', value: '2 hours ago', icon: Clock, color: 'text-blue-500' },
            { label: 'Data Sync', value: 'Up to date', icon: Shield, color: 'text-purple-500' }
          ].map((status, i) => (
            <div key={i} className="bg-slate-900 p-4 rounded-xl border border-slate-800 flex items-center gap-4">
              <status.icon className={status.color} size={20} />
              <div>
                <p className="text-xs text-slate-400">{status.label}</p>
                <p className="text-sm font-bold">{status.value}</p>
              </div>
            </div>
          ))}
        </div>

      <CalendarModal isOpen={isCalendarOpen} onClose={() => setIsCalendarOpen(false)} events={upcomingEvents} />
    </div>
  );
}

const CalendarModal = ({ isOpen, onClose, events }: { isOpen: boolean; onClose: () => void; events: any[] }) => {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm z-50 flex items-center justify-center p-4 animate-fadeIn">
      <div className="bg-slate-900 border border-slate-700 rounded-2xl w-full max-w-2xl overflow-hidden shadow-2xl">
        {/* Modal Header */}
        <div className="flex justify-between items-center p-6 border-b border-slate-800">
          <h3 className="text-xl font-black text-white">June 2026</h3>
          <div className="flex items-center gap-4">
            <button className="text-slate-400 hover:text-white"><ChevronLeft /></button>
            <button className="text-slate-400 hover:text-white"><ChevronRight /></button>
            <button onClick={onClose} className="p-2 bg-slate-800 rounded-full hover:bg-rose-500 hover:text-white transition-colors ml-4">
              <X size={20} />
            </button>
          </div>
        </div>

        {/* Modal Body: Events List */}
        <div className="p-6 space-y-4 max-h-[60vh] overflow-y-auto">
          {events.map((event: any) => (
            <div key={event.id} className="flex gap-4 items-center bg-slate-950 p-4 rounded-xl border border-slate-800">
              <div className="w-16 h-16 rounded-xl bg-blue-900/30 text-blue-500 flex flex-col items-center justify-center font-bold">
                <span className="text-sm">JUN</span>
                <span className="text-xl">{event.date.split(' ')[1].replace(',', '')}</span>
              </div>
              <div>
                <h4 className="text-white font-bold text-lg">{event.title}</h4>
                <p className="text-slate-400 text-sm mt-1">Type: {event.type}</p>
              </div>
            </div>
          ))}
          {/* Example of adding a new event button inside the modal */}
          <button className="w-full py-4 border-2 border-dashed border-slate-700 text-slate-400 rounded-xl hover:border-blue-500 hover:text-blue-500 transition-colors font-bold flex justify-center items-center gap-2">
            Add New Event
          </button>
        </div>
      </div>
    </div>
  );
};
